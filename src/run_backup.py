#!/usr/bin/python
"""Upload images on iPhoto library to Dropbox
"""
from __future__ import division, print_function

import argparse
import contextlib
import json
import os
import subprocess
import sys
import time
from configparser import ConfigParser
from datetime import datetime
from io import TextIOWrapper  # pylint: disable=W0611
from multiprocessing.pool import ThreadPool
from typing import Text

import dropbox
import exifread
import requests
import six
import urllib3

if six.PY2:
    from six.moves import input  # pylint: disable=C0412

APP_KEY = ""
APP_SECRET = ""
ACCESS_TOKEN = ""
IPTHOTO_LIBRARY_ROOT = os.path.expanduser(
    "~/Pictures/Photos Library.photoslibrary/Masters/")

MAX_UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024
MAX_RETRY_COUNT_ON_NETWORK_ERROR = 10

FFPROBE_COMMAND = "ffprobe -v quiet -print_format json -show_format -show_streams"


def _get_video_info(video_file_path):
    command = FFPROBE_COMMAND.split(' ')
    command.append(video_file_path)
    raw_json = subprocess.check_output(command)
    return json.loads(raw_json)


def _get_image_info(image_file_path):
    return exifread.process_file(open(image_file_path, 'rb'))


def _get_image_creation_time(image_file_path):
    image_info = _get_image_info(image_file_path)
    if "EXIF DateTimeOriginal" in image_info:
        str_time = str(image_info["EXIF DateTimeOriginal"])
    elif "Image DateTime" in image_info:
        str_time = str(image_info["Image DateTime"])
    elif "DateTimeOriginal" in image_info:
        str_time = str(image_info["DateTimeOriginal"])
    else:
        return None

    return datetime.strptime(str_time, "%Y:%m:%d %H:%M:%S")


def _get_video_creation_date(vodeo_file_path):
    video_info = _get_video_info(vodeo_file_path)
    if not video_info:
        return None
    if ('format' not in video_info or
            'tags' not in video_info['format'] or
            'creation_time' not in video_info['format']['tags']):
        return None
    return datetime.strptime(
        video_info['format']['tags']['creation_time'], "%Y-%m-%dT%H:%M:%S.%fZ")


@contextlib.contextmanager
def stopwatch(message):
    """Context manager to print how long a block of code took."""
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        print('Total elapsed time for %s: %.3f' %
              (message, end_time - start_time))


def _dropbox_auth(key, secret, token):
    """Login to dropbox then return Dropbox object

    Returns:
        dropbox.Dropbox -- Dropbox object
    """

    if not token:
        auth_flow = dropbox.DropboxOAuth2FlowNoRedirect(key, secret)

        authorize_url = auth_flow.start()
        print("1. Go to: " + authorize_url)
        print("2. Click \"Allow\" (you might have to log in first).")
        print("3. Copy the authorization code.")
        auth_code = input("Enter the authorization code here: ").strip()

        try:
            oauth_result = auth_flow.finish(auth_code)
        except dropbox.oauth.NotApprovedException as exception:
            print('Error: %s' % (exception,))
            return None
        token = oauth_result.access_token
    return dropbox.Dropbox(token)


def _get_server_file_size(dbx, server_path):
    try:
        metadata = dbx.files_get_metadata(server_path)
        return metadata.size
    except dropbox.exceptions.ApiError:
        return 0


def _get_server_file_media_info(dbx, server_path):
    try:
        metadata = dbx.files_get_metadata(server_path, include_media_info=True)
        return metadata.media_info
    except dropbox.exceptions.ApiError:
        return None


def _auto_retry(func, *args, **kwargs):
    retry_count = 0
    while True:
        try:
            return func(*args, **kwargs)
        except (requests.exceptions.ConnectionError,
                urllib3.exceptions.ProtocolError):
            retry_count += 1
            if retry_count == MAX_RETRY_COUNT_ON_NETWORK_ERROR:
                print("Error: Failed operation {}() due to Network error {}").format(
                    func.__name__, sys.exc_info()
                )
                raise
            else:
                print("Warning: Network error {}() {} [Retry: {}/{}]".format(
                    func.__name__, sys.exc_info(), retry_count,
                    MAX_RETRY_COUNT_ON_NETWORK_ERROR), file=sys.stderr)


def _chunk_upload(dbx, server_path, local_full_path, file_obj, file_size):
    # type: (dropbox.Dropbox, Text, Text, TextIOWrapper, int) -> Text
    uploaded_file_path = ""
    data = file_obj.read(MAX_UPLOAD_CHUNK_SIZE)
    upload_session_start_result = _auto_retry(
        dbx.files_upload_session_start, data)

    cursor = dropbox.files.UploadSessionCursor(session_id=upload_session_start_result.session_id,
                                               offset=file_obj.tell())
    commit = dropbox.files.CommitInfo(
        path=server_path, autorename=True, mode=dropbox.files.WriteMode('add', None))

    while file_obj.tell() < file_size:
        if (file_size - file_obj.tell()) <= MAX_UPLOAD_CHUNK_SIZE:
            data = file_obj.read(MAX_UPLOAD_CHUNK_SIZE)
            metadata = _auto_retry(dbx.files_upload_session_finish, data,
                                   cursor,
                                   commit)
            uploaded_file_path = metadata.path_lower
        else:
            data = file_obj.read(MAX_UPLOAD_CHUNK_SIZE)
            _auto_retry(dbx.files_upload_session_append, data,
                        cursor.session_id,
                        cursor.offset)
            cursor.offset = file_obj.tell()
        print("Uploaded {} bytes for {} ({:.0%})".format(
            cursor.offset, local_full_path, cursor.offset/file_size))
    return uploaded_file_path


def _get_svr_path_from_metadata(local_full_path):
    ext = os.path.splitext(local_full_path)[1].lower()
    time_stamp = _get_video_creation_date(local_full_path) if ext == '.mov' else \
        _get_image_creation_time(local_full_path)
    if not time_stamp:
        print("Failed to get metadata from {}".format(local_full_path))
        return None

    return "/Photos/{}/{:02}/{:02}/{}".format(
        time_stamp.year,
        time_stamp.month,
        time_stamp.day,
        os.path.basename(local_full_path)
    )


def upload(params):
    """Upload thread function"""
    dbx = dropbox.Dropbox
    (
        is_delete,
        dbx,
        path,
        file_name,
        server_path
    ) = params
    local_full_path = os.path.join(path, file_name)
    dest_full_path = "{}/{}".format(server_path, file_name)
    file_size = os.path.getsize(local_full_path)
    meta_svr_path = _get_svr_path_from_metadata(local_full_path)
    if _get_server_file_size(dbx, dest_full_path) == file_size or \
            (meta_svr_path and _get_server_file_size(dbx, meta_svr_path) == file_size):
        print("{} already exists. skipping...".format(dest_full_path))
    # else:
    #     exif_tags = exifread.process_file(
    #         open(local_full_path, 'rb'), stop_tag='DateTimeOriginal')
    #     if 'EXIF DateTimeOriginal'
    else:
        uploaded_full_path = dest_full_path
        with stopwatch("Uplaoding {} to {} ({} bytes)".format(file_name, server_path, file_size)):
            with open(local_full_path, 'rb') as local_file:
                try:
                    if file_size > MAX_UPLOAD_CHUNK_SIZE:
                        uploaded_full_path = _chunk_upload(dbx, dest_full_path,
                                                           local_full_path, local_file, file_size)
                    else:
                        metadata = _auto_retry(dbx.files_upload, local_file.read(),
                                               dest_full_path, autorename=True)
                        uploaded_full_path = metadata.path_lower
                except dropbox.exceptions.ApiError as error:
                    print("{} was not loaded to {} due to ApiError:{}".format(
                        file_name, server_path, error))
                    uploaded_full_path = None
                except OSError:
                    print("File read failed {} {}".format(
                        local_full_path, sys.exc_info()), file=sys.stderr)
                    raise
        if not uploaded_full_path:
            return
        uploaded_size = _get_server_file_size(dbx, uploaded_full_path)
        if uploaded_size != file_size:
            raise Exception("The uploaded file size doesn't match: {} vs {} svr:{} local:{}".format(
                uploaded_size, file_size, uploaded_full_path, local_full_path
            ))
    if is_delete:
        print("Deleting file: {}".format(local_full_path))
        os.remove(local_full_path)


def _fix_file(dbx, metadata):
    if isinstance(metadata, dropbox.files.FileMetadata) and metadata.media_info and metadata.media_info.is_metadata():
        media = metadata.media_info.get_metadata()
        if media.time_taken:
            dest = "/Photos/{}/{:02}/{:02}/{}".format(
                media.time_taken.year,
                media.time_taken.month,
                media.time_taken.day,
                metadata.name)
            src = metadata.path_lower
            print("Moving from {} to {}".format(src, dest))
            try:
                dbx.files_move(src, dest)
            except dropbox.exceptions.ApiError as error:
                print("Skipping {} since the destination file already exists. {}". format(
                    src, error))

    else:
        print(type(metadata))


def _fix_server_path(dbx, subpath):
    # type (dropbox.Dropbox) -> None
    root_path = '/Photos'
    if subpath:
        root_path += "/{}".format(subpath)
    result = dbx.files_list_folder(
        root_path, include_media_info=True)
    for entry in result.entries:
        _fix_file(dbx, entry)
    while result.has_more:
        result = dbx.files_list_folder_continue(result.cursor)
        for entry in result.entries:
            _fix_file(dbx, entry)


def _parse_args():
    parser = argparse.ArgumentParser(
        description='Upload iPhoto library to Dropbox')
    parser.add_argument(
        '--folder', help='`yyyy/mm/dd` format sub folder on iPhoto Library to upload. If not set, upload all folders')
    parser.add_argument('--delete', action='store_true',
                        help="Delete local files after uploading to Dropbox")
    parser.add_argument("--threads", default=1, type=int,
                        help="Number of max upload thread")
    parser.add_argument('--fix', action='store_true',
                        help="Move files directly under Photos to <yyyy/mm/dd> according to the exifs")
    return parser.parse_args()


def main():
    """Dropbox iPhoto library uploader main function
    """

    args = _parse_args()

    key = APP_KEY
    secret = APP_SECRET
    token = ACCESS_TOKEN
    config = ConfigParser()
    config.read('run_backup.ini')
    if 'auth' in config:
        key = config['auth']['key'] if 'key' in config['auth'] else None
        secret = config['auth']['secret'] if 'secret' in config['auth'] else None
        token = config['auth']['access_token'] if 'access_token' in config['auth'] else None

    dbx = _dropbox_auth(key, secret, token)
    dbx.users_get_current_account()

    if not os.path.exists(IPTHOTO_LIBRARY_ROOT):
        print("Cannot find iPhoto Library path on {}".format(
            IPTHOTO_LIBRARY_ROOT), file=sys.stderr)
        exit(1)

    if args.fix:
        _fix_server_path(dbx, args.folder)
        return

    paths = os.walk(IPTHOTO_LIBRARY_ROOT)
    for path, _, files in paths:
        sub_path = path[path.find("Masters/") + len("Masters/"):]
        server_path = "/Photos/{}".format(sub_path[:sub_path.rfind('/')])

        if not args.folder or sub_path.startswith(args.folder):
            if files:
                print("{} files under {}".format(len(files), path))
                with stopwatch("Uploading {} files under {}".format(len(files), path)):
                    pool = ThreadPool(args.threads)
                    params = []
                    for file_name in files:
                        params.append(
                            [args.delete, dbx, path, file_name, server_path])

                    pool.map(upload, params)
                    pool.close()
                    pool.join()

            if args.delete:
                try:
                    os.rmdir(path)
                    print("Directory {} was removed".format(path))
                except OSError:
                    print("Local directory `{}` was not deleted since it's not empty".format(
                        path), file=sys.stderr)


def _main_test():
    date = _get_video_creation_date(
        "/Users/sekim/Pictures/Photos Library.photoslibrary/Masters/2011/09/14/20110914-164130/IMG_0511.MOV")
    print(date)
    exif = exifread.process_file(open(
        "/Users/sekim/Pictures/Photos Library.photoslibrary/Masters/2011/09/14/20110914-164130/IMG_0510.JPG",
        'rb'
    ))
    print(exif["Image DateTime"])
    print(exif["EXIF DateTimeOriginal"])


if __name__ == '__main__':
    main()
