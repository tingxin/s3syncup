import os
import argparse
from helper import conf
from helper import move_s3

def sync():
    move_s3.sync()

def rename():
    print("rename done")
    pass

def del_bucket():
    print("del_bucket done")
    pass


def main():
    conf.load_env()
    parser = argparse.ArgumentParser(description="跨账户迁移s3数据")
    parser.add_argument("--sync", action="store_true", help="同步s3文件")
    parser.add_argument("--rename", action="store_true", help="重命名s3桶名称(会新建桶，并删除老的，文件将自动搬运到新的桶)")
    parser.add_argument("--delete", action="store_true", help="删除桶并且清空桶内数据")

    args = parser.parse_args()

    if args.sync:
        names = move_s3.list_buckets_name()
        confirm = input(f"以下S3 桶中的内容要进行迁移：\n{names}。\n你确定要继续吗？(y/n): ")
        if confirm.lower() == 'y':
            move_s3.sync()
    if args.delete:
        names = move_s3.list_buckets_name()
        confirm = input(f"以下S3 桶中及里面的内容要被删除：\n{names}。\n你确定要继续吗？(y/n): ")
        if confirm.lower() == 'y':
            print("delete")
    if args.rename:
        print("delete")

if __name__ == "__main__":
    main()