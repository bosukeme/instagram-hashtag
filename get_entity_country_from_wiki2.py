import pickle
import wikipedia
import random
import pandas as pd
import numpy as np
from ast import literal_eval
import nltk
#import enchant
import pycountry
from bs4 import BeautifulSoup
from bs4.element import Comment
import urllib.request
import re
import os

country_alpha_file_path = os.path.join(os.getcwd(), 'country_alpha_dict.pickle')
alpha_country_file_path = os.path.join(os.getcwd(), 'alpha_country_dict.pickle')
print(country_alpha_file_path)
print(alpha_country_file_path)

def get_wiki_url_from_search_term(word):
    """
    Search a word meaning on wikipedia.
    """
    wikipedia.set_lang('en')
    results = wikipedia.search(word)    

    # get first result
    if results:
        page = wikipedia.page(results[0])
        url = page.url
        summary = page.summary
        content = page.content
    else:
        url = 'NA'
        summary = 'NA'
        content = 'NA'
    return url



"""
Functions related to the wiki_entity_country_from_url.py
"""
def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True

def text_from_html(body):
    soup = BeautifulSoup(body, 'html.parser')
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)  
    return u" ".join(t.strip() for t in visible_texts)

def get_entity_points(index):
    points = 0
    if 0 <= index < 5:
        points = 10
    elif 5<= index < 10:
        points = 5
    elif 10 <= index < 20:
        points = 2
    elif index > 20:
        points = 1
    return points

# Get the total points for the entity across the whole article
def get_keyword_total_points(keyword, headline, text):
    import nltk
    sentences = nltk.sent_tokenize(text)
    points = 0
    if keyword in headline:
        points += 20
    for i in range(len(sentences)):
        temp = sentences[i]
        if keyword in temp:
            pnt = get_entity_points(i)
            points += pnt
    return points


def get_top_5_entity_countries(country_list, text): # there is a slight bug here in cases where an country may have multiple variations within the text, like united states for example, however its giving the right answer so we good
        
    country_points_list = []
    top_5_country_list = []
    for country in country_list:
        points = get_keyword_total_points(country, '', text)
        country_points_list.append(points)
    sorted_ind = sorted(range(len(country_points_list)), key=lambda k: country_points_list[k], reverse=True)
    sorted_country_list = [country_list[i] for i in sorted_ind]
    sorted_points_list = [country_points_list[i] for i in sorted_ind]
    
    try:
        top_5_countries = sorted_country_list[0:5]
        coverage = round(sorted_points_list[0]/sum(sorted_points_list),2)
        for countries in top_5_countries:
            top_5_country_list.append(countries)
                
        return top_5_country_list, coverage
    except:
        return ['Unknown'], 0

def get_name_from_wiki_url(url): # Strips out the name of an entity from its Wikipedia URL
    text = re.search(r'(?<=wiki/)[^.\s]*',url)
    names1 = text.group(0)
    names2 = names1.split('_')
    if len(names2) > 1:
        names2 = ' '.join(names2)
    else:
        names2 = names2[0]
    return names2

def generate_country_alpha_dicts(): # This generates a dict of world countries and maps to their 2-digit ISO codes
    import pycountry
    #Create dictionary of countries and their 2 name list
    gb_countries = ['England', 'Scotland', 'Northern Ireland', 'Wales']
    country_names_list = []
    country_alpha_list = []
    for country in gb_countries:
        country_names_list.append(country)
        country_alpha_list.append('GB')
    
    # Accomodate for other ways america is said (may need to add some more exceptions here)
    us_countries = ['America', 'U.S', 'U.S.A']
    for country in us_countries:
        country_names_list.append(country)
        country_alpha_list.append('US')
    
    for country in pycountry.countries:
        country_names_list.append(country.name)
        country_alpha_list.append(country.alpha_2)
    
    # Add in the edge case for when its unknown
    country_names_list.append('Unknown')
    country_alpha_list.append('Unknown')
    
    country_alpha_dict = dict(zip(country_names_list, country_alpha_list))
    alpha_to_country_dict = dict(zip(country_alpha_list, country_names_list))
    
    # Save the country_alpha dict, and alpha_country dict into a pickle *** Change this filename to your current one
    # with open('/Users/Edidiong Wilson/Desktop/gitlab/country_alpha_dict.pickle', "wb") as output_file:
    with open(country_alpha_file_path, "wb") as output_file:
         try:
            pickle.dump(country_alpha_dict, output_file)
         except EOFError:
            pass
            
    
    # with open('/Users/Edidiong Wilson/Desktop/gitlab/alpha_country_dict.pickle', "wb") as output_file:
    with open(alpha_country_file_path, "wb") as output_file:
         pickle.dump(alpha_to_country_dict, output_file)
    
    return None



def get_infobox_text(soup): # This extracts all the text from the infobox card of a wiki page
    infobox_list = ['infobox', 'infobox vcard', 'infobox biography vcard', 'infobox geography vcard']
    for vcards in infobox_list: # loops through a number of infobox vcards to find the one that works
        try:
            info_text = soup.find_all("table",vcards)[0]
            if len(info_text) > 0:
                break
        except:
            continue
    try:
        col_len = len(info_text)
    except:
        info_text = []
    return info_text

def get_entity_country_from_infobox(url): # Get the country of an entity from just the infobox text
    try:
        html = urllib.request.urlopen(url).read()
        soup = BeautifulSoup(html,'lxml')
    
    except:
        text = re.search(r'(?<=wiki/)[^.\s]*',url)
        encoded_name = urllib.parse.quote(text.group(0))
        url = url.replace(text.group(0),encoded_name)
        html = urllib.request.urlopen(url).read()
        soup = BeautifulSoup(html,'lxml')

    
    info_text = get_infobox_text(soup)
    
    if len(info_text) > 0:
        info_text_list = []
        for cols in info_text:
            info_text_list.append(cols.text)
        
        text = '. '.join(info_text_list)
        
        # with open('/Users/Edidiong Wilson/Desktop/gitlab/country_alpha_dict.pickle', "rb") as input_file:
        with open(country_alpha_file_path, "rb") as input_file:
            country_alpha_dict = pickle.load(input_file)

        # Add Tanzania to the mix    
        country_alpha_dict.update({'Tanzania':'TZ'})
        
        country_list_full = []
        
        for country in country_alpha_dict.keys(): # Find a list of countries appearing in the text
            if country in text:
                country_list_full.append(country)
        
        countries, confidence = get_top_5_entity_countries(country_list_full, text)
        
        for country in countries: # CLean up the list of countries
            for cntry in countries:
                if country in cntry:
                    if country != cntry:
                        countries.remove(country)
                
        country_final = [] # Convert the countries into their 2 digits ISO codes
        for country in countries:
            country_final.append(country_alpha_dict[country])
        #print(country_final)

        if len(country_final) > 0:
            unique_countries = list(set(country_final))
            return unique_countries[0], confidence, unique_countries[1:], unique_countries
        else:
            return None
    else:
        return None

def get_entity_country_from_wiki_text(url):

    try:
        name = get_name_from_wiki_url(url)
        text = wikipedia.summary(name, sentences=2)
    except:
        try:
            html = urllib.request.urlopen(url).read()
            text = text_from_html(html)
        except:
            text = re.search(r'(?<=wiki/)[^.\s]*',url)
            encoded_name = urllib.parse.quote(text.group(0))
            url = url.replace(text.group(0),encoded_name)
            html = urllib.request.urlopen(url).read()
            text = text_from_html(html)

    # with open('/Users/Edidiong Wilson/Desktop/gitlab/country_alpha_dict.pickle', "rb") as input_file:
    with open(country_alpha_file_path, "rb") as input_file:
        country_alpha_dict = pickle.load(input_file)

    # Add Tanzania to the mix    
    country_alpha_dict.update({'Tanzania':'TZ'})
    
    country_list_full = []
    for country in country_alpha_dict.keys():
        if country in text:
            country_list_full.append(country)
    
    countries, confidence = get_top_5_entity_countries(country_list_full, text)
    #print(countries)
    
    country_final = [] # Convert the countries into their 2 digits ISO codes
    for country in countries:
        country_final.append(country_alpha_dict[country])
    #print(country_final)

    if len(country_final) > 0:
        unique_countries = list(set(country_final))
        return unique_countries[0], confidence, unique_countries[1:], unique_countries
    else:
        return None

def get_wiki_country(url):
    name = get_name_from_wiki_url(url)
    try:
        main_country, confidence, other_countries, all_countries = get_entity_country_from_infobox(url)
        if len(all_countries) > 2:
            main_country, confidence, other_countries, all_countries = get_entity_country_from_wiki_text(url)
    except:
        main_country, confidence, other_countries, all_countries = get_entity_country_from_wiki_text(url)
    
    if len(all_countries) > 2:
        main_country = 'Multiple'
        other_countries = all_countries
    return main_country, confidence, other_countries, all_countries

def get_wiki_summary(url):
    #print(url) 
    try:
        name = get_name_from_wiki_url(url)
#        print(name)
        summ = wikipedia.summary(name, sentences=3)
#        print('yass1')
    except:
#        print('yass2')
        try:
            text = re.search(r'(?<=wiki/)[^.\s]*',url)
            encoded_name = urllib.parse.quote(text.group(0))
            url2 = url.replace(text.group(0),encoded_name)
            html = urllib.request.urlopen(url2).read()
            text = text_from_html(html)
            sentences = nltk.sent_tokenize(text)
            summ = ' '.join(sentences[2:4])
        except:
            summ = 'Not Available'
        
    return summ




"""
Objective: To build functionality that allows you to put in a search term, and 
get the wikipedia page link for that search term, then analyse the wikipedia page
for the search term and determine what country the individual is from

- Get the country
- Look at the country of its most related entities and see if it agrees with what you have determined
"""
## Load country_alpha and alpha_country dicts
try:
    # with open('/Users/Edidiong Wilson/Desktop/gitlab/country_alpha_dict.pickle', "rb") as input_file:
    with open(country_alpha_file_path, "rb") as input_file:
        country_alpha_dict = pickle.load(input_file)
except:
    generate_country_alpha_dicts()
    # with open('/Users/Edidiong Wilson/Desktop/gitlab/country_alpha_dict.pickle', "rb") as input_file:
    with open(country_alpha_file_path, "rb") as input_file:
        country_alpha_dict = pickle.load(input_file)
    print('Country dicts have been generated and saved')
    
try:
    # with open('/Users/Edidiong Wilson/Desktop/gitlab/alpha_country_dict.pickle', "rb") as input_file:
    with open(alpha_country_file_path, "rb") as input_file:
         alpha_to_country_dict = pickle.load(input_file)
except:
    generate_country_alpha_dicts()
    # with open('/Users/Edidiong Wilson/Desktop/gitlab/alpha_country_dict.pickle', "rb") as input_file:
    with open(alpha_country_file_path, "rb") as input_file:
         alpha_to_country_dict = pickle.load(input_file)
    print('Country dicts have been generated and saved')
    
def get_entity_country_from_wikipedia(entity):
    
    url = get_wiki_url_from_search_term(entity)

    # Bespoke function to get summary built by myself that allows for error handling if the wikipedia approach doesnt work
    summ = get_wiki_summary(url)
#    print(summ)

    # Run the function that takes a wikipedia URL and return the that the entity belongs to (@Nas make sure you look into this and work to ensure you understand how it works)
    main_country, confidence, other_countries, all_countries = get_wiki_country(url)
    
    return main_country, all_countries, summ, url
    
get_entity_country_from_wikipedia("football")