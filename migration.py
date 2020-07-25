
import datetime
import flickr_api
import httplib2
import json
import os
import piexif
import requests
import shutil
import time
import traceback
from PIL import Image
from googleapiclient.discovery import build
from oauth2client.client import flow_from_clientsecrets, save_to_well_known_file
from oauth2client.file import Storage
from oauth2client.tools import run_flow

TMP_DIR = os.path.join(os.path.dirname(__file__), "tmp")
FLICKR_USERNAME = "micronar"
PAGE_LIMIT = 50
IMG_SIZE = "X-Large 4K"
SCOPE_URL = "https://www.googleapis.com/auth/photoslibrary"
TOKEN_URL = "https://www.googleapis.com/oauth2/v4/token"
ALBUM_URL = "https://photoslibrary.googleapis.com/v1/albums/"

class Migration:
	def __init__(self, flickr_secret_file, google_secret_file, google_cred_file):
		flickr_json = json.load(open(flickr_secret_file))
		flickr_api.set_keys(api_key=flickr_json['api_key'], api_secret=flickr_json['api_secret'])
		
		self.credentials = self.get_credential(google_secret_file, google_cred_file)
		self.service = build('photoslibrary', 'v1', credentials=self.credentials)
		self.photo_map = {}
		self.album_map = {}
		self.tag_map = {}
		self.datetime_map = {}
		self.google_album_map = {}
		self.refresh_time = datetime.datetime.now()

		self.get_album_list()
		return

	def get_credential(self, google_secret_file, google_cred_file):
		http = httplib2.Http()
		cred_obj = Storage(google_cred_file)
		credentials = cred_obj.get()
		
		if credentials is None or credentials.invalid:
			print ("Getting new credential file")
			flow = flow_from_clientsecrets(google_secret_file, scope=SCOPE_URL)

			credentials = run_flow(flow, cred_obj, http=http)
		elif credentials.access_token_expired:
			print ("Token expired, now refresh")
			credentials.refresh(http)

		self.refresh_time = datetime.datetime.now()

		return credentials

	def get_album_list(self):
		nextPageToken = ""
		continue_download = True

		headers = {
			'Authorization': "Bearer " + self.service._http.request.credentials.access_token,
			'Content-Type': 'application/json',
		}

		while continue_download:
			r = requests.get(ALBUM_URL + "?pageSize=50" + ("" if nextPageToken == "" else "&pageToken=" + nextPageToken), headers=headers)
			album_dic = json.loads(r.content)
			if "nextPageToken" in album_dic:
				nextPageToken = album_dic["nextPageToken"]
			else:
				continue_download = False
			for row in album_dic["albums"]:
				self.google_album_map[row["title"]] = row["id"]

		# print(self.google_album_map)
		return

	def upload_photo(self, filename):
		f = open(filename, 'rb').read()

		url = 'https://photoslibrary.googleapis.com/v1/uploads'
		headers = {
			'Authorization': "Bearer " + self.service._http.request.credentials.access_token,
			'Content-Type': 'application/octet-stream',
			'X-Goog-Upload-Content-Type': "mime-type",
			'X-Goog-Upload-File-Name': os.path.basename(filename).encode('utf-8'),
			'X-Goog-Upload-Protocol': "raw",
		}


		r = requests.post(url, data=f, headers=headers)
		upload_token = r.content.decode("utf-8") 
		
		return upload_token

	def update_google_items(self, filenames, token_map):

		url = 'https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate'
		items = []

		for filename in filenames:
			items.append(
				{
					"description": ' '.join(self.tag_map[filename]),
					"simpleMediaItem": {
						"fileName": os.path.basename(filename),
						"uploadToken": token_map[filename]
					}  
				})

		body = { 'newMediaItems' : items }

		bodySerialized = json.dumps(body)
		headers = {
			'Authorization': "Bearer " + self.service._http.request.credentials.access_token,
			'Content-Type': 'application/json',
		}

		r = requests.post(url, headers=headers, data=bodySerialized)
		result_dic = json.loads(r.content)
		print(result_dic)

		result_item_map = {}
		for row in result_dic["newMediaItemResults"]:
			result_item_map[row["uploadToken"]] = row["mediaItem"]

		self.add_to_album(token_map, result_item_map)
		return 

	def add_to_album(self, filename_token_map, result_item_map):
		album_id_map = {}
		headers = {
			'Authorization': "Bearer " + self.service._http.request.credentials.access_token,
			'Content-Type': 'application/json',
		}

		for filename, token in filename_token_map.items():
			for album_name in self.album_map[filename]:
				album_id = self.get_album_id(album_name)
				if album_id in album_id_map:
					album_id_map[album_id].append(result_item_map[token]["id"])
				else:
					album_id_map[album_id] = [result_item_map[token]["id"]]
		
		for album_id, ids in album_id_map.items():
			res = requests.post(ALBUM_URL + album_id + ":batchAddMediaItems", headers=headers, data=json.dumps({"mediaItemIds":ids}) )
		return

	def google_upload(self, page=0):

		if (datetime.datetime.now() - self.refresh_time).total_seconds() > 3000:
			print("Refresh token");
			http = httplib2.Http()
			self.credentials.refresh(http)
			self.refresh_time = datetime.datetime.now()

		bulk_list = []
		token_map = {}
		if page == 0:
			for (dirpath, _, filenames) in os.walk(TMP_DIR):
				for fl in filenames:
					if fl != ".DS_Store":
						fl = os.path.join(dirpath, fl)
						bulk_list.append(fl)
						token = self.upload_photo(fl)
						token_map[fl] = token
		else:
			print ("Uploading page %s"  % page)
			ul_folder = os.path.join(TMP_DIR, str(page))
			for fl in os.listdir(ul_folder):
				fl = os.path.join(ul_folder, fl)
				bulk_list.append(fl)
				token = self.upload_photo(fl)
				token_map[fl] = token

		self.update_google_items(bulk_list, token_map)
		return

	def get_album_id(self, album_name):
		headers = {
			'Authorization': "Bearer " + self.service._http.request.credentials.access_token,
			'Content-Type': 'application/octet-stream',
			'X-Goog-Upload-Content-Type': "mime-type",
			'X-Goog-Upload-Protocol': "raw",
		}
		if (album_name in self.google_album_map):
			return self.google_album_map[album_name]
		else:
			r = requests.post(ALBUM_URL, headers=headers, data=json.dumps({"album":{"title":album_name}}))
			album_json = json.loads(r.content)
			self.google_album_map[album_json["title"]] = album_json["id"]
			return album_json["id"]

	def update_date_taken(self, photo_path, date_taken):
		try:
			os.system('SetFile -d "{}" {}'.format(date_taken.strftime('%m/%d/%Y %H:%M:%S'), photo_path))
			os.system('SetFile -m "{}" {}'.format(date_taken.strftime('%m/%d/%Y %H:%M:%S'), photo_path))

			exif_dict = piexif.load(photo_path)
			if piexif.ExifIFD.DateTimeOriginal not in exif_dict:
				exif_dict['Exif'] = {piexif.ExifIFD.DateTimeOriginal: date_taken.strftime("%Y:%m:%d %H:%M:%S"), **exif_dict['Exif']}
				exif_bytes = piexif.dump(exif_dict)
				piexif.insert(exif_bytes, photo_path)

			self.datetime_map[photo_path] = date_taken
			return True
		except:
			return False

	def flickr_download(self, dl_folder, photos):
		retry = 5
		while retry > 0:
			try:
				for photo in photos:
					print(photo)

					date_taken = datetime.datetime.strptime(flickr_api.Photo.getInfo(photo)["taken"], "%Y-%m-%d %H:%M:%S")
					
					photo_path = os.path.join(dl_folder, "%s.jpg" % str(photo["id"] if photo["title"] == '' else photo["title"]))

					self.album_map[photo_path] = [ x["title"] for x in photo.getAllContexts()[0]  ]
					self.tag_map[photo_path] = [ x["raw"] for x in photo.getTags() ]
					
					sizes = photo.getSizes()
					url = sizes[IMG_SIZE]["source"] if IMG_SIZE in sizes else sizes[list(sizes.keys())[-1]]["source"]
					if "play" in url:
						continue

					while not os.path.exists(photo_path):
						print ("Getting from %s" % url)
						res = requests.get(url)
						open(photo_path, "wb").write(res.content)
					
						if not self.update_date_taken(photo_path, date_taken):
							print("Remove invalid file and retry")
							os.remove(photo_path)

				return
			except:
				print("Sleep and retry after 1 min")
				retry = retry - 1
				time.sleep(60)

		raise Exception("Too many retries")
		return

	def migrate(self):
		user = flickr_api.Person.findByUserName(FLICKR_USERNAME)

		limit = int(user.getInfo()["photos_info"]["count"]/PAGE_LIMIT)+1
		print("Total page: %s" % limit)
		for page in range(limit, 0, -1):
			try:
				dl_folder = os.path.join(TMP_DIR, str(page))
				if os.path.exists(dl_folder):
					if len(os.listdir(dl_folder)) == 0:
						continue
				else:
					os.makedirs(dl_folder)

				print ("Getting page %s" % page)
				photos = user.getPhotos(page=page, per_page=PAGE_LIMIT)
				self.flickr_download(dl_folder, photos)
				self.google_upload(page)

				for fl in os.listdir(dl_folder):
					fl = os.path.join(dl_folder, fl)
					os.remove(fl)
					
				# return
			except:
				traceback.print_exc()
				# shutil.rmtree(dl_folder) 
				return
			
		return

if __name__ == "__main__":
	FLICKR_CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "flickr_secret.json")
	GOOGLE_SECRET_FILE = os.path.join(os.path.dirname(__file__), "client_secret.json")
	GOOGLE_CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "credentials.storage")

	x = Migration(FLICKR_CREDENTIALS_FILE, GOOGLE_SECRET_FILE, GOOGLE_CREDENTIALS_FILE)
	x.migrate()


	