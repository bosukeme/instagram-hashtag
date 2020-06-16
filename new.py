import os
import numpy as np
import datetime
import pandas as pd    
from pymongo import MongoClient
import pandas as pd
from nltk.corpus import stopwords
import nltk
import instaloader 
import get_entity_country_from_wiki2 as ent_cntry
#nltk.download('stopwords')
from dotenv import load_dotenv

load_dotenv()

#MONGO_URL = os.environ['MONGO_URL']
MONGO_URL='mongodb://Bloverse:uaQTRSp6d9czpcCg@64.227.12.212:27017/social_profiling?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false'

client= MongoClient(MONGO_URL)
db = client.hashtag

pd_path=os.path.join(os.getcwd(), 'Bloverse_Data_Articles_and_Entities.csv')
#pd_path=os.path.join(os.getcwd(), 'trying_this2.csv')

pass_in_df=pd.read_csv(pd_path).head(25) ####this is the only part you will modify in this code
#print(pass_in_df)

#json_df=pass_in_df.to_json('json_df.json')


def initiate_hashtag_mongo_instance():
    
    #client = MongoClient('localhost') # you can use this to connect to the url of a db thats hosted somewhere else... this is how Ukeme will be running things based on guidance from Otse
    db = client.hashtag 

    # Load in the instagram_hashtag collection from MongoDB
    instagram_term_hashtag_collection = db.instagram_term_hashtag_collection # similarly if 'testCollection' did not already exist, Mongo would create it

    # See how many entries we had at the start
    cur = instagram_term_hashtag_collection.find()
    print('We had %s term hashtags at the start' % cur.count())
    
    
    return instagram_term_hashtag_collection
    


def save_collection_to_csv(collection, path):
    # Get all the values in the collection into a generator
    cur = collection.find()

    # Read the collection into a df
    df =  pd.DataFrame(list(cur))
    
    # Save the df to a csv
    df.to_csv(path)
    
    
def get_top_5_hashtags(search_string):
    """
    This takes any search string and then queries instagram to find 
    """
    L = instaloader.Instaloader()
    hashtag_list = []
    hashtags = instaloader.TopSearchResults(L.context, search_string)

    for val in hashtags.get_hashtag_strings():
        hashtag_list.append(val)
    
    return hashtag_list[0:5]


def get_hashtag_metrics(hashtag):
    L = instaloader.Instaloader()
    curr_time = datetime.datetime.now()
    # Get the posts for the hashtag as a generator
    hasthag_posts = L.get_hashtag_posts(hashtag)

    like_list = []
    comment_list = []
    days_list = []

    # Loop through the latest 5 post for the hashtag and calculate metrics
    count = 0
    
    for val in hasthag_posts:
        if count < 10:
            diff = curr_time - val.date_local
            days_list.append(diff.days)
            like_list.append(val.likes)
            comment_list.append(val.comments)
            count += 1
        else:
            break
    
    ## Get the averages of all the key metrics
    like_avg = abs(int(np.average(like_list)))
    comment_avg = abs(int(np.average(comment_list)))
    days_avg = max(abs(float(np.average(days_list))), 0.01) # if this value is 0, we divide it by 0.01 so that the hashtag_score does not throw errors
    
    hashtag_score = round(((like_avg + (comment_avg*3))/days_avg), 2)
    
    return like_avg, comment_avg, days_avg, hashtag_score


def get_term_hashtag_df(term):
    """
    This takes an entity and then gets the 5 most popular hashtags for the entity as
    well as metrics like avg_likes etc.
    """
    term_name_list = []
    term_hashtag_list = []
    term_hashtag_avg_likes_list = []
    term_hashtag_avg_comments_list = []
    term_hashtag_avg_days_list = []
    term_hashtag_score_list = []
    
    # Get the time of run
    curr_time = datetime.datetime.now()

    hashtags = get_top_5_hashtags(term)
#     print(hashtags)
#     print()
    
    if len(hashtags) > 0:
        for tags in hashtags:
    #         print('Hashtag: #%s' % tags)
            like_avg, comment_avg, days_avg, hashtag_score = get_hashtag_metrics(tags)
    #         print('Avg Likes: %s \nAvg Comments: %s \nAvg Days: %s \nHashtag Score: %s' % (like_avg, comment_avg, days_avg, hashtag_score))
            term_name_list.append(term)
            term_hashtag_list.append(tags)
            term_hashtag_avg_likes_list.append(like_avg)
            term_hashtag_avg_comments_list.append(comment_avg)
            term_hashtag_avg_days_list.append(days_avg)
            term_hashtag_score_list.append(hashtag_score)

        term_hashtag_df = pd.DataFrame()
        term_hashtag_df['Term Name'] = term_name_list
        term_hashtag_df['Term Hashtag'] = term_hashtag_list
        term_hashtag_df['Term Hashtag Avg Likes'] = term_hashtag_avg_likes_list
        term_hashtag_df['Term Hashtag Avg Comments'] = term_hashtag_avg_comments_list
        term_hashtag_df['Term Hashtag Avg Days'] = term_hashtag_avg_days_list
        term_hashtag_df['Term Hashtag Score'] = term_hashtag_score_list
        term_hashtag_df['Processed Time'] = curr_time
    
    else:
        term_name_list.append(term)
        term_hashtag_list.append('NA')
        term_hashtag_avg_likes_list.append('NA')
        term_hashtag_avg_comments_list.append('NA')
        term_hashtag_avg_days_list.append('NA')
        term_hashtag_score_list.append('NA')
        
        term_hashtag_df = pd.DataFrame()
        term_hashtag_df['Term Name'] = term_name_list
        term_hashtag_df['Term Hashtag'] = term_hashtag_list
        term_hashtag_df['Term Hashtag Avg Likes'] = term_hashtag_avg_likes_list
        term_hashtag_df['Term Hashtag Avg Comments'] = term_hashtag_avg_comments_list
        term_hashtag_df['Term Hashtag Avg Days'] = term_hashtag_avg_days_list
        term_hashtag_df['Term Hashtag Score'] = term_hashtag_score_list
        term_hashtag_df['Processed Time'] = curr_time
#         print(entity_hashtag_df.head())

    return term_hashtag_df





def search_for_hashtag_term(name, term_hashtag_collection):
    """
    This function searches for an entity in the named_entities_collection
    and returns the country of the entity if its found
    """
    from ast import literal_eval
    try:
        term_dict = term_hashtag_collection.find_one({'Term Name':name})
        return term_dict
    except:
        try:
            term_dict = literal_eval(term_hashtag_collection.find_one({'Term Name':name}))
            return term_dict
        except:
            return None

def clean_df_columns(df):
    """
    Cleans out 'Unnamed' columns from the dataframe so we can easily save the df to mongoDB
    """
    cols = list(df.columns)

    final_cols = []

    for col in cols:
        if 'Unnamed' not in col:
            final_cols.append(col)

    df = df[final_cols]
    
    return df

def update_term_hashtag_collection(term_hashtag_df, term_hashtag_collection):
    """
    This function takes the term hashtag df after the run and saves it to our mongoDB instance
    """

    term_hashtag_df = clean_df_columns(term_hashtag_df)
    term_hashtag_collection.insert_many(term_hashtag_df.to_dict('records'))
          
    return None


def get_article_terms(title, entities, stopwords):
    # Build functionality that gets the title words and filters out any entity names
    # Then wraps them together into a list of article terms
    ent_token_list = []

    for ents in entities:
        words = ents.split()
        for word in words:
            ent_token_list.append(word.lower())

    title_words = title.split()
    title_words = [word for word in title_words if word.lower() not in ent_token_list]
    title_words = [word for word in title_words if len(word) > 1]
    title_words = [word for word in title_words if word not in stopwords.words('english')]
    title_words = [word for word in title_words if word.lower() not in stopwords.words('english')]
   
    article_terms = []
    article_terms += title_words
    article_terms += entities
    
    return article_terms


def get_article_term_hashtags_and_update_collection(article_terms, term_hashtag_collection):
    """
    This function gets the hashtag for any new terms that have been found in the article which we did not have before
    in our hashtag collection
    """
    for term in article_terms:
        print('Term: %s' % term)
        term_dict = search_for_hashtag_term(term, term_hashtag_collection)
        
        try:
            if term_dict is None:
                print('Getting hashtags for %s' % term)
                term_hashtag_df = get_term_hashtag_df(term)
                update_term_hashtag_collection(term_hashtag_df, term_hashtag_collection)
            else:
                print('Term already existed')
                print()
        except:
            pass
    
    return None   
    

    
    
term_hashtag_collection = initiate_hashtag_mongo_instance()


def pass_df_and_generate_hashtags(df):
    
    for title, entity in df[['title', 'entities']].itertuples(index=False):
        article_terms = get_article_terms(title, eval(entity), stopwords)
        print(article_terms)
        get_article_term_hashtags_and_update_collection(article_terms, term_hashtag_collection)
        
        

##this adds all the entities we have to a collection 

def add_entities_to_collection(df):
    #df=pd.read_csv('Bloverse Data - Articles and Entities.csv')
    
    ###remove the stringed list to a list of list
    unpr_ent=[]
    for a in df['entities']:
        a=eval(a)
        unpr_ent.append(a)
    
    ###convert the list of list to a single list having the entities
    entity=[item for sublist in unpr_ent for item in sublist]

    # Load in the instagram_hashtag collection from MongoDB
    preprocessed_entity_collection = db.preprocessed_entity_collection # similarly if 'testCollection' did not already exist, Mongo would create it
    
    ###save the entities to the preprocessed collection
    df =  pd.DataFrame()
    df['entity']=entity
    preprocessed_entity_collection.insert_many(df.to_dict('records'))


add_entities_to_collection(pass_in_df)
 

def search_for_spacy_entity(name, preprocessed_entity_collection):
    """
    This function searches for an entity in the named_entities_collection
    and returns the country of the entity if its found
    """

    # Load in the instagram_hashtag collection from MongoDB
    preprocessed_entity_collection = db.preprocessed_entity_collection # similarly if 'testCollection' did not already exist, Mongo would create it
    
    try:
        entity =(preprocessed_entity_collection.find_one({'entity':name})['entity'])
        #print(entity)
        return entity
    except:
        return None
#print(search_for_spacy_entity('Sampson Ahi', preprocessed_entity_collection))


def update_spacy_named_entities_collection(latest_entity_df, preprocessed_entity_collection):
    """
    This function takes the named entity df after the run and saves it to our mongoDB instance
    """
    # See how many entries we had at the start
    cur = preprocessed_entity_collection.find()
    print('We had %s spacy entity entries at the start' % cur.count())
    
    latest_entity_df = clean_df_columns(latest_entity_df)
    #print(latest_entity_df)
    
    # Update the entity collection
   # preprocessed_entity_collection.insert_many(latest_entity_df.to_dict('records'))

    # See how many entries we had at the start
    cur = preprocessed_entity_collection.find()
    print('We have %s spacy entity entries at the end' % cur.count())
          
    return None



#df=pd.read_csv('Bloverse Data - Articles and Entities.csv')
    
###remove the stringed list to a list of list
#unpr_ent=[]
#for a in df['entities']:
 #   a=eval(a)
  #  unpr_ent.append(a)

###convert the list of list to a single list having the entities
#entity=[item for sublist in unpr_ent for item in sublist]

def get_spacy_entity_metadata(entity_list_full):
    
   # import get_entity_country_from_wiki as ent_cntry

    ## Now deal with the latest entity_list and update the entity dict
    new_unique_entities = list(set(entity_list_full))
    preprocessed_entity_collection = db.preprocessed_entity_collection
    processed_entity_collection = db.processed_entity_collection

    entity_list = []
    entity_country_list = []
    entity_summary_list = []
    entity_url_list = []
    reject_entities=[]
    for i in range(len(new_unique_entities)):
        if i%2 == 0:
            print('%s of %s' % (i, len(new_unique_entities)))
        t = i
        ents = new_unique_entities[t]
      
        entity_country = search_for_spacy_entity(ents, preprocessed_entity_collection) #searches the named entity collection to see if our entity is found
        
        
        processed_entity=list(processed_entity_collection.find({},{ "_id": 0, "Entity": 1}))
        processed_entity=list((val for dic in processed_entity for val in dic.values()))
        if entity_country  not in processed_entity:
            try:
                entity_country, entity_all_countries, entity_summary, entity_url = ent_cntry.get_entity_country_from_wikipedia(ents)
                entity_list.append(ents)
                entity_country_list.append(entity_all_countries)
                entity_summary_list.append(entity_summary)
                entity_url_list.append(entity_url)

            except Exception as e:
                reject_entities.append(ents)
                continue
        else:
            print('already processed'}
    named_entity_df = pd.DataFrame()
    named_entity_df['Entity'] = entity_list
    named_entity_df['Entity Country'] = entity_country_list
    named_entity_df['Entity Summary'] = entity_summary_list
    named_entity_df['Entity Wiki Url'] = entity_url_list
    
    print(named_entity_df)
    return named_entity_df




###saving processed entities



def save_processed_entities(named_entity_df):
    #df=pd.read_csv('Bloverse Data - Articles and Entities.csv')

    # Load in the instagram_hashtag collection from MongoDB
    processed_entity_collection = db.processed_entity_collection # similarly if 'testCollection' did not already exist, Mongo would create it
    
    cur = processed_entity_collection.find() ##check the number before adding
    print('We had %s spacy entity entries at the start' % cur.count())
    
     ##search for the entities in the processed colection and store it as a list
    processed_entity=list(processed_entity_collection.find({},{ "_id": 0, "Entity": 1}))
    processed_entity=list((val for dic in processed_entity for val in dic.values()))


    #loop throup the entity metadata, and add only new enteries
    for entity in named_entity_df['Entity']:
        if entity  not in processed_entity:
            processed_entity_collection.insert_many(named_entity_df.to_dict('records')) ####save the df to the collection
    
    
    
   
    #processed_entity_collection.insert_many(named_entity_df.to_dict('records')) ####save the df to the collection
  
    cur = processed_entity_collection.find() ##check the number after adding
    print('We have %s spacy entity entries at the end' % cur.count())



#####delete the processed entities from the preprocessed 

def delete_processed_entity():

    # Load in the instagram_hashtag collection from MongoDB
    preprocessed_entity_collection = db.preprocessed_entity_collection 
    processed_entity_collection=db.processed_entity_collection
    
    ##search for the entities in the preprocessed colection and store it as a list
    preprocessed_entity=preprocessed_entity_collection.find({},{ "_id": 0, "entity": 1})
    preprocessed_entity=list((val for dic in preprocessed_entity for val in dic.values()))
    
    ##search for the entities in the processed colection and store it as a list
    processed_entity=list(processed_entity_collection.find({},{ "_id": 0, "Entity": 1}))
    processed_entity=list((val for dic in processed_entity for val in dic.values()))

    ###loop throup and delete entities in preprocessed if they have been processed
    for item in preprocessed_entity:
        if item in processed_entity:
            preprocessed_entity_collection.delete_one({'entity':item})
        else:
            pass
        
    print("We are done with the process")



def future_hashtags_entity(df):
    
    
    ###remove the stringed list to a list of list
    unpr_ent=[]
    for a in df['entities']:
        a=eval(a)
        unpr_ent.append(a)

    ###convert the list of list to a single list having the entities
    entity=[item for sublist in unpr_ent for item in sublist]
    
    
    
    term_hashtag_collection = initiate_hashtag_mongo_instance() ## initiate mongodb and check the number of items
    
    pass_df_and_generate_hashtags(df) ### generate hash tags for the titles and entities
    
    add_entities_to_collection(df) # add the entities to the preprocessed collection
    
    named_entity_df=get_spacy_entity_metadata(entity) ### process the entities by getting its meta data 
    
    save_processed_entities(named_entity_df) ## save the entity meta data to the processed collections
    
    delete_processed_entity() ##delete processed entities from the preprocessed ones
    named_entity_df = named_entity_df.to_json()
    
    return named_entity_df
    
future_hashtags_entity(pass_in_df) ###call the function by passing a df. Make sure df have 'title' and 'entities' features


