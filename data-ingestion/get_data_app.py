#!/usr/bin/env python


# [START app]
import os
import logging
import get_data

import flask
import google.cloud.storage as gcs

# [start config]
app = flask.Flask(__name__)
# Configure this environment variable via app.yaml
CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']
#
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
# [end config]

@app.route('/')
def welcome():
         return '<html><a href="get data">get all data </a> dpd data</html>'

@app.route('/ingest')
def ingest_all_data():
    try:
         # verify that this is a cron job request
         is_cron = flask.request.headers['X-Appengine-Cron']
         logging.info('Received cron request {}'.format(is_cron))

         # download data
         url = 'www.dallasopendata.com'
         outfile = 'currentdata.csv'
         status = 'scheduled ingest of {} to {}'.format(url, outfile)
         logging.info(status)
         get_data.download_save(domain=url, file_name=outfile)

         # upload to cloud storage
         client = gcs.Client()
         bucket = client.get_bucket(CLOUD_STORAGE_BUCKET)
         blob = gcs.Blob('crime/currentdata.csv', bucket)
         blob.upload_from_filename(outfile)

         # change permissions
         blob.make_public()
         status = 'uploaded {} to {}'.format(outfile, blob.name)
         logging.info(status)

    except KeyError as e:
         status = '<html>Sorry, this capability is accessible only by the Cron service, but I got a KeyError for {} -- try invoking it from <a href="{}"> the GCP console / AppEngine / taskqueues </a></html>'.format(e, 'http://console.cloud.google.com/appengine/taskqueues?tab=CRON')
         logging.info('Rejected non-Cron request')

    return status

@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
# [END app]