import logging
from upload_data import upload_data

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  upload_data.run()
