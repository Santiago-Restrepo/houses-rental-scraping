import os
from dotenv import load_dotenv
load_dotenv()

MAX_LISTINGS_PER_PAGE = 50
BASE_URL=os.environ['BASE_URL']
RENTAL_BASE_URL=os.environ['RENTAL_BASE_URL']