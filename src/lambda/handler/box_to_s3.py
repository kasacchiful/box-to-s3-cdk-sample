from box_sdk_gen import BoxClient, BoxJWTAuth, JWTConfig, Authentication
import boto3
import os
import shutil
from io import BufferedIOBase
from pathlib import Path

## Box Authentication
def boxAuth() -> Authentication:
    box_param_key = os.environ.get('BOX_PARAM_KEY')
    ssm_client = boto3.client('ssm')
    ## Get SSM Parameter Store
    ssm_param = ssm_client.get_parameter(
        Name=box_param_key,
        WithDecryption=True,
    )
    jwt_key_config = ssm_param["Parameter"]["Value"]
    ## Set Box Auth Config
    config = JWTConfig.from_config_json_string(jwt_key_config)
    auth = BoxJWTAuth(config)
    return auth

## Get bucket name and key name from s3 url
def split_s3_path(s3_path: str) -> tuple[str, str]:
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket = path_parts.pop(0)
    key = "/".join(path_parts)
    return bucket, key

## Main Hander
def lambda_handler(event, context):
    auth: BoxJWTAuth = boxAuth()
    box_client: BoxClient = BoxClient(auth=auth)

    ## configure
    box_folder_id = str(event.get('input_box_folder_id'))
    s3_url = event.get('output_s3_url')
    s3_bucket, s3_objpath = split_s3_path(s3_url)

    ## init tmp dir
    tmp_dir = os.path.join("/tmp", "etl_sample")
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    os.makedirs(tmp_dir)

    ## get files
    filelist = []
    items = box_client.folders.get_folder_items(box_folder_id).entries
    for item in items:
        if item.type == 'file':
            ## ファイルだけダウンロード対象とする
            file_name = item.name
            file_path = os.path.join(tmp_dir, file_name)
            ## Write the Box file contents to tmp storage
            file_content_stream: BufferedIOBase = box_client.downloads.download_file(file_id=item.id)
            with open(file_path, 'wb') as f:
                shutil.copyfileobj(file_content_stream, f)
            print(f"Downloaded File: '{file_name}'")
            filelist.append({
                'id': item.id,
                'name': item.name,
                'download_name': file_name,
                'download_path': os.path.abspath(file_path),
            })

    ## upload to s3
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    for file in filelist:
        download_filepath = file.get('download_path')
        download_filename = file.get('download_name')
        objkey = os.path.join(s3_objpath, download_filename)
        bucket.upload_file(download_filepath, objkey)
        print(f"Uploaded File: 's3://{s3_bucket}/{objkey}'")

    ## remove tmp
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    return {
        'statusCode': 200,
    }

if __name__ == '__main__':
    event = {
        'input_box_folder_id': '0',
        'output_s3_url': 's3://box-file-bucket-test/box/etl-sample/',
    }
    lambda_handler(event, None)