import os
from dotenv import load_dotenv
load_dotenv()

MAX_LISTINGS_PER_PAGE = os.environ['MAX_LISTINGS_PER_PAGE']
PAGE_SLEEP_TIME = os.environ['PAGE_SLEEP_TIME']
BASE_URL=os.environ['BASE_URL']
RENTAL_BASE_URL=os.environ['RENTAL_BASE_URL']