import random
import string
import boto3

def s3_create_bucket(name):
    client = boto3.client('s3')
    response = client.create_bucket(ACL='public-read-write', Bucket=name)
    print(response)
    print(response['Location'])


# delete_bucket()
def s3_delete_bucket(name):
    client = boto3.client('s3')
    response = client.delete_bucket(Bucket=name)
    print(response)


# delete_bucket_tagging()

def s3_delete_bucket_tagging(name):
    client = boto3.client('s3')
    response = client.delete_bucket_tagging(Bucket=name)
    print(response)


# list_buckets()
def s3_list_buckets():
    client = boto3.client('s3')
    response = client.list_buckets()
    print(response)

# list_objects()
def s3_list_objects(name):
    client = boto3.client('s3')
    response = client.list_objects(Bucket=name)
    keys=[]
    for i in response['Contents']:
        #print('contents : ',i['Key'])
        keys.append({'Key': i['Key']})
    print(keys)
    return keys


# list_objects_v2()

def s3_list_objects_v2(name):
    client = boto3.client('s3')
    response = client.list_objects_v2(Bucket=name)
    for i in response['Contents']:
        print('contents : ',i['Key'])


def s3_put_bucket_tagging(name):
    client = boto3.client('s3')
    response = client.put_bucket_tagging(Bucket=name, Tagging={
        'TagSet': [
            {
                'Key': 'Name',
                'Value': 'image'
            },
        ]
    })

# get_bucket_accelerate_configuration()
def s3_get_bucket_accelerate_configuration(name):
    client = boto3.client('s3')
    response = client.get_bucket_accelerate_configuration(Bucket='test-upload-mona')
    print('response',response)

# put_bucket_accelerate_configuration()

def s3_put_bucket_accelerate_configuration(name):
    client = boto3.client('s3')
    response = client.put_bucket_accelerate_configuration(Bucket=name,
    AccelerateConfiguration = {
        'Status': 'Suspended'
    })
    print(response)

# put_bucket_intelligent_tiering_configuration()
# def s3_put_bucket_intelligent_tiering_configuration(name):
#     client = boto3.client('s3')
#     response = client.s3_put_bucket_intelligent_tiering_configuration(
#         Bucket=name,
#         Id='string',
#         IntelligentTieringConfiguration={
#             'Id': 'string',
#             'Filter': {
#                 'Prefix': 'string',
#                 'Tag': {
#                     'Key': 'string',
#                     'Value': 'string'
#                 },
#                 'And': {
#                     'Prefix': 'string',
#                     'Tags': [
#                         {
#                             'Key': 'string',
#                             'Value': 'string'
#                         },
#                     ]
#                 }
#             },
#             'Status': 'Enabled' | 'Disabled',
#             'Tierings': [
#                 {
#                     'Days': 123,
#                     'AccessTier': 'ARCHIVE_ACCESS' | 'DEEP_ARCHIVE_ACCESS'
#                 },
#             ]
#         }
#
#     )
#     print(response)

# put_object()
def s3_put_object(name):
    client = boto3.client('s3')
    response=client.put_object(
        Bucket=name,
        Body='/Users/monasekar/PycharmProjects/pythonProject/textfilesample.txt',
        Key='textfilesample.txt'
    )['ETag']
    print(response)

# upload_file()

def s3_upload_file(name):
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('/Users/monasekar/PycharmProjects/pythonProject/textfilesample.txt', name, 'test/hello.txt')
    print('uploaded file using resource than client')

# upload_fileobj()

def s3_upload_fileobj(name):
    s3 = boto3.client('s3')
    with open('/Users/monasekar/PycharmProjects/pythonProject/textfilesample.txt', 'rb') as data:
        s3.upload_fileobj(data, name, 'test/hello2.txt')
        print('uploaded a binary mode file')

# copy()
def s3_copy(name):
    s3 = boto3.resource('s3')
    copy_source = {
        'Bucket': name,
        'Key': 'test/hello2.txt'
    }
    s3.meta.client.copy(copy_source, name, 'hello3.txt')
    print('Uploaded from one s3 location to another')

# copy_object()
def s3_copy_object(name):
    client = boto3.client('s3')
    response = client.copy_object(Bucket=name,
    CopySource={'Bucket': 'test-upload-mona', 'Key': 'hello3.txt'},
    Key='hello.txt')
    print('copied an obj')

# get_object()
def s3_get_object(name):
    client = boto3.client('s3')
    response = client.get_object(Bucket=name, Key='hello.txt')
    print('Content type:', response['ContentType'],'\nContentLength:', response['ContentLength'])

# download_file()
def s3_download_file(name):
    s3 = boto3.resource('s3')
    s3.meta.client.download_file(name, 'hello.txt', '/tmp/hello.txt')
    print('download the file to tmp directory')

# download_fileobj()
def s3_download_fileobj(name):
    client = boto3.client('s3')
    with open('new.txt', 'wb') as data:
        client.download_fileobj(name, 'hello.txt', data)
    print('writing file to local in binary mode from s3')

# generate_presigned_post()
def s3_generate_presigned_post(bucketname,objectname,expiry):
    client = boto3.client('s3')
    response = client.generate_presigned_post(bucketname, objectname,
    Fields=None, Conditions=None, ExpiresIn=expiry)
    print(response)

# generate_presigned_url()
def s3_generate_presigned_url(bucketname,objectname,expiry):
    client = boto3.client('s3')
    url=client.generate_presigned_url('get_object',Params={'Bucket': bucketname,'Key': objectname},
                                                  ExpiresIn=expiry)
    print(url)


def s3_multipart_upload(name):
    client = boto3.client('s3')
    response = client.create_multipart_upload(Bucket=name, Key='multipart.txt')
    upload_id = response['UploadId']
    print('UploadId:', upload_id, '\nBucket:', response['Bucket'], '\nKey:',response['Key'])
    partNumber = 1
    chunkSize = 50*1024*1024
    with open('/Users/monasekar/PycharmProjects/pythonProject/textfile.txt', 'rb') as data:
        piece = data.read(chunkSize)
        etag=[]
        while len(piece)>0:
            res = client.upload_part(Body=piece, Bucket=name,Key='multipart.txt',PartNumber=partNumber,
            UploadId=upload_id)
            etag.append({'PartNumber': partNumber, 'ETag': res['ETag']})
            partNumber += 1
            piece = data.read(chunkSize)
            print(etag)
    completeparts = client.complete_multipart_upload(Bucket=name, Key='multipart.txt',
                                                     MultipartUpload={'Parts': etag},
                                                     UploadId=upload_id)
    print(completeparts['Location'])
# create_multipart_upload()
# upload_part()
# complete_multipart_upload()

# delete_object()
def s3_delete_object(bucketname,objectname):
    client = boto3.client('s3')
    response = client.delete_object(Bucket=bucketname, Key=objectname)
    print('Successfully deleted one object:', objectname)

# delete_objects()
def s3_delete_objects(name):
    objects=s3_list_objects(name)
    client = boto3.client('s3')
    response = client.delete_objects(Bucket=name, Delete={'Objects':objects})
    for i in response['Deleted']:
        print(i['Key'])

#'Marker': 'string',

def s3_operation_invoker(operation):
    if (operation == "create_bucket"):
        s3_create_bucket("mona-test-boto3")
    elif (operation == "delete_bucket"):
        s3_delete_bucket("cf-templates-18mxdovqivteu-us-east-1")
    elif (operation == "delete_bucket_tagging"):
        s3_delete_bucket_tagging("test-upload-mona")
    elif(operation=="put_bucket_tagging"):
        s3_put_bucket_tagging("test-upload-mona")
    elif (operation=="list_buckets"):
        s3_list_buckets()
    elif (operation=="list_objects"):
        keys=s3_list_objects("test-upload-mona")
    elif (operation == "list_objects_v2"):
        s3_list_objects_v2("test-upload-mona")
    elif (operation == "get_bucket_accelerate_configuration"):
        s3_get_bucket_accelerate_configuration("test-upload-mona")
    elif (operation == "put_bucket_accelerate_configuration"):
        s3_put_bucket_accelerate_configuration("test-upload-mona")
    elif (operation == "put_bucket_intelligent_tiering_configuration"):
        #s3_put_bucket_intelligent_tiering_configuration("test-upload-mona")
        print('')

    elif (operation == "put_object"):
        s3_put_object("test-upload-mona")
    elif (operation == "upload_file"):
        s3_upload_file("test-upload-mona")
    elif (operation == "upload_fileobj"):
        s3_upload_fileobj("test-upload-mona")
    elif (operation == "copy"):
        s3_copy("test-upload-mona")
    elif (operation == "copy_object"):
        s3_copy_object("hello7654")
    elif (operation == "get_object"):
        s3_get_object("hello7654")
    elif (operation == "download_file"):
        s3_download_file("hello7654")
    elif (operation == "download_fileobj"):
        s3_download_fileobj("hello7654")
    elif (operation == "generate_presigned_post"):
        s3_generate_presigned_post('hello7654', 'hello.txt', 3600)
    elif (operation == "generate_presigned_url"):
        s3_generate_presigned_url('hello7654', 'hello.txt', 3600)
    elif (operation == "multipart_upload"):
        s3_multipart_upload("hello7654")
    elif (operation == "delete_object"):
        s3_delete_object("hello7654","multipart.txt")
    elif (operation == "delete_objects"):
        s3_delete_objects("cf-templates-18mxdovqivteu-us-east-1")
    else:
        print("no operations matched in boto3 s3 API")

if __name__ == '__main__':

    s3_operation_invoker("delete_bucket")

