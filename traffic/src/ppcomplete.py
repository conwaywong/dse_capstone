import pickle
import pprint
BASE_DIR = "/video/dse_capstone/traffic"
PICKLE_FILENAME = BASE_DIR + "/completed_files.pkl"
f = open(PICKLE_FILENAME, 'rb')
completedFiles = pickle.load(f)

pprint.pprint(completedFiles)

# If desired to restart a district uncomment and set
#del completedFiles['station_5min']['2015']['10']
#pickle.dump(completedFiles, open(PICKLE_FILENAME, "wb"), 2)
