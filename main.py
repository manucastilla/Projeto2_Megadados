import boto3
import io

from gzip import GzipFile
from re import findall
from urllib.parse import urlparse

from pyspark import SparkContext
from warcio.archiveiterator import ArchiveIterator

def read_br_content(shard_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('commomcrawl')
    content_list = []
    with io.BytesIO() as f:
        bucket.download_fileobj(shard_name, f)
        f.seel(io.SEEK_SET)
        for record in ArchiveIterator(f):
            if record.rec_type == 'conversion':
                uri = record.rec_headers['WARC-TARGET-URI']
                parsed_uri = urlparse(uri)
                if parsed_uri.netloc.endswith('.br'):
                    content = record.content_strem()\
                        .read().decode('utf-8')
                    content_list.append((uri, content))
    return content_list

if __name__ == "__main__":
    sc = SparkContext(appName = "Teste")
    sc \ 
        .textFile("s3://megadados-alunos/wet.paths") \
        .repartition(sc.defaultParallelism) \
        .flatMap(read_br_content)
        .saveAsSequenceFile("s3://megadados-alunos/web-brasil")
    sc.stop()