import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import os
import logging
from logging.handlers import RotatingFileHandler
import time

logger = logging.getLogger(__name__)

# 设置日志级别
logger.setLevel(logging.INFO)
# 创建一个handler，用于写入日志文件
handler = RotatingFileHandler('move_s3.log', maxBytes=100000, backupCount=3)
logger.addHandler(handler)


# 创建一个handler，用于将日志输出到控制台
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

# 定义日志格式
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
handler.setFormatter(formatter)
console_handler.setFormatter(formatter)



def copy_s3_bucket(s3_src, s3_dest,src_bucket_name, dest_bucket_name):
    # 获取源桶中的所有对象
    logger.info(f"开始从桶{src_bucket_name}复制文件到{dest_bucket_name}...")
    all_objects = list_s3_objects(s3_src, src_bucket_name)
    logger.info(f"当前桶{src_bucket_name}的文件有:\n{all_objects}")
    try:
        for file_name in all_objects:
            try:
                if file_name.endswith("/"):
                    s3_dest.put_object(Bucket=dest_bucket_name, Key=file_name)
                    logger.info(f"create folder {file_name} to {dest_bucket_name}")
                else:
                    local_path = f"{os.getcwd()}/{os.getenv('temp_folder')}/{src_bucket_name}/{file_name}"
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    s3_src.download_file(src_bucket_name, file_name, local_path)
                    logger.info(f"download {file_name} to {local_path}")
                    s3_dest.upload_file(local_path, dest_bucket_name, file_name)
                    logger.info(f"Copied {file_name} to {dest_bucket_name}")
            except Exception as exx:
                logger.error(f"同步文件 {file_name} 失败: {exx}")
               
    except NoCredentialsError:
        logger.error("Credentials not available")
    except ClientError as e:
        logger.error(f"An ClientError occurred: {e}")
    except Exception as ex:
        logger.error(f"An error occurred: {ex}")


def delete_s3_bucket(s3, bucket_name):
    try:
        # 确保桶是空的
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            print("Bucket is not empty. Deleting objects first.")
            s3.delete_objects(Bucket=bucket_name, Delete={'Objects': [{'Key': obj['Key']} for obj in response['Contents']]})

        # 删除桶
        s3.delete_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} deleted successfully.")
    except NoCredentialsError:
        logger.error("Credentials not available")
    except ClientError as e:
        logger.error(f"An error occurred: {e}")


def list_s3_buckets(s3_client):
    # 创建一个 S3 客户端
    try:
        # 列出所有桶
        response = s3_client.list_buckets()
        buckets = response['Buckets']
        return [bucket['Name'] for bucket in buckets if os.getenv("filter_prefix") == "" or bucket['Name'].startswith(os.getenv("filter_prefix"))]
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return []
  
def ensure_bucket_exists(s3, bucket_name, region_name):  
    """  
    检查指定名称的S3桶是否存在，如果不存在则创建它。  
  
    :param bucket_name: 要检查的S3桶的名称  
    :param region_name: S3桶所在的区域，默认为'us-east-1'  
    :return: None  
    """  
   
    logger.info(f"正在检查桶:{bucket_name}")
    try:  
        # 尝试获取桶，如果桶不存在，这将抛出NoSuchBucket异常  
        s3.head_bucket(Bucket=bucket_name)  
        return True 
    except ClientError as e:  
        # 如果错误代码是NoSuchBucket，则桶不存在  
        error_code = int(e.response['Error']['Code'])  
        if error_code == 404:  
            logger.info(f"Bucket {bucket_name} does not exist. Creating it now...")  
            # 创建桶  
            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region_name})  
            return True  
        else:  
            # 如果不是NoSuchBucket错误，则重新抛出异常  
            logger.error(f"failed to find and create bucket:{bucket_name}")
            return False 

def list_s3_objects(s3, bucket_name): 
    paginator = s3.get_paginator('list_objects_v2')

    # 初始化分页器
    page_iterator = paginator.paginate(Bucket=bucket_name)

    all_objects = []

    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                all_objects.append(obj['Key'])

        if 'CommonPrefixes' in page:
            for prefix in page['CommonPrefixes']:
                all_objects.append(prefix['Prefix'])

    return all_objects


def delete_files_in_directory(directory):
    # 遍历目录
    for root, dirs, files in os.walk(directory, topdown=False):
        for name in files:
            # 构造完整的文件路径
            file_path = os.path.join(root, name)
            # 删除文件
            os.remove(file_path)

def list_buckets_name():
    s3_src= boto3.client('s3', 
                         region_name=os.getenv("src_region"),
                         aws_access_key_id=os.getenv("src_access_key"), 
                         aws_secret_access_key=os.getenv("src_secret_key")) 
    
    bucket_names = list_s3_buckets(s3_src)
    logger.info(f"prepare move those buckets:{bucket_names}")
    return bucket_names

def sync():
    # 检查目录是否存在
    if not os.path.exists(os.getenv('temp_folder')):
        # 如果目录不存在，则创建目录
        os.makedirs(os.getenv('temp_folder'))
        logger.info(f"目录 {os.getenv('temp_folder')} 已创建。")
    else:
        # 如果目录已存在，则打印消息
        logger.info(f"目录 {os.getenv('temp_folder')} 已存在。")

    delete_files_in_directory(os.getenv('temp_folder'))


    s3_dest= boto3.client('s3', region_name=os.getenv("dest_region")) 

    s3_src= boto3.client('s3', 
                         region_name=os.getenv("src_region"),
                         aws_access_key_id=os.getenv("src_access_key"), 
                         aws_secret_access_key=os.getenv("src_secret_key")) 
    
    bucket_names = list_s3_buckets(s3_src)
    logger.info(f"prepare move those buckets:{bucket_names}")
    for bucket_name in bucket_names:
        dest_bucket_name = f"{bucket_name}-{os.getenv('temp_prefix')}"

        ensure_bucket_exists(s3_dest,dest_bucket_name,os.getenv("dest_region"))

        copy_s3_bucket(s3_src, s3_dest, bucket_name, dest_bucket_name)

    logger.info(f"success to move those buckets:{bucket_names}")

def clear(s3, bucket_name):
    try:
        # 获取桶中所有对象
        delete_objects = list_s3_objects(s3, bucket_name)
        if delete_objects:
            # 准备删除对象的请求
            # 发送删除请求
            s3.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_objects})
            print(f"All objects in bucket {bucket_name} have been deleted.")
        else:
            print(f"Bucket {bucket_name} is already empty.")
    except Exception as e:
        print(f"An error occurred: {e}")

def move(s3, src_bucket_name, dest_bucket_name):
    move_objects = list_s3_objects(s3, src_bucket_name)
    for key in move_objects:
        try:
            s3.copy_object(Bucket=dest_bucket_name, CopySource={'Bucket': src_bucket_name, 'Key': key}, Key=key)
        except Exception as ex:
            logger.error(f"failed to move {key} from {src_bucket_name}  to {dest_bucket_name}")

        

def delete_bucket(s3, bucket_name):
    try:
        s3.delete_bucket(Bucket=bucket_name)
    except Exception as ex:
        logger.error(f"failed to delete {bucket_name}")

def move_all():
    s3_dest= boto3.client('s3', region_name=os.getenv("dest_region")) 
    bucket_names = list_buckets_name()
    for bucket_name in bucket_name:
        backup_bucket_name = f"{bucket_name}-{os.getenv('temp_prefix')}"
        move(s3_dest,backup_bucket_name, bucket_name)

