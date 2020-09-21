from urllib import request
import json
import boto3
import logging
import traceback

logger = logging.getLogger('MezamashiUranaiLogger')
logger.setLevel( logging.INFO )

def remove_html_tags(text):
    import re
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

try:
    dynamodb = boto3.resource( 'dynamodb', region_name = 'us-east-1' )
    table = dynamodb.Table( 'mezamashi-uranai' )

    src_url = 'https://www.fujitv.co.jp/meza/uranai/data/uranai.json'

    with request.urlopen( src_url ) as resp:
        body = json.loads( resp.read() )
        dt = body['date']
        with table.batch_writer() as writer:
            for rank in body['ranking']:
                table.delete_item(
                    Key = {
                        'sign': rank['name']
                    }
                )
                writer.put_item(
                    Item = {
                        'sign': rank['name'],
                        'date': dt,
                        'rank': rank['rank'],
                        'message': remove_html_tags( rank['text'] ),
                        'advice': rank['advice'],
                        'point': rank['point']
                    }
                )
                logger.info("{0}: {1}".format( rank['rank'], rank['name'] ) )

except:
    logger.error( traceback.format_exc() )
    raise Exception( traceback.format_exc() )
