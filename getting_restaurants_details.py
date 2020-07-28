import scrapy
import json
import csv
import urllib.parse

class YelpPeachtreeCornersSpider(scrapy.Spider):
    name = "yelp_props"

    def start_requests(self):

        unique_businesses = [{'Restaurant Name': 'Maritza & Frank’s Southern Cooking', 'Avg Price ($)': 2, 'Categories': 'Southern, Breakfast & Brunch, American (Traditional)', 'Address*': '715 Industrial Blvd', 'Phone number*': '(770) 892-0270', 'businessUrl': '/biz/maritza-and-franks-southern-cooking-mcdonough', 'bizId': 'rN82o_VvTQKZOewMQnzYRg', 'Sub address': []}, {'Restaurant Name': 'Papa’s Pizza To Go', 'Avg Price ($)': '', 'Categories': 'Pizza, Italian, Breakfast & Brunch', 'Address*': '135 N Peachtree St', 'Phone number*': '(706) 359-3663', 'businessUrl': '/biz/papas-pizza-to-go-lincolnton', 'bizId': 'LOjfuHKBo8fI9WToytdQrA', 'Sub address': []}, {'Restaurant Name': 'East Bound And Grounds', 'Avg Price ($)': '', 'Categories': 'Coffee & Tea, Breakfast & Brunch, Cupcakes', 'Address*': '216 Main St', 'Phone number*': '(678) 402-7602', 'businessUrl': '/biz/east-bound-and-grounds-dallas', 'bizId': 'Lk557vrkXETMJt1WxhNMQQ', 'Sub address': []}, {'Restaurant Name': 'Coconut’s', 'Avg Price ($)': 1, 'Categories': 'Restaurants', 'Address*': '116 Lee Ave', 'Phone number*': '(803) 943-3399', 'businessUrl': '/biz/coconuts-hampton', 'bizId': 'zTZwCz5SU2vZ9eg6n61iVw', 'Sub address': []}, {'Restaurant Name': 'Chick-fil-A', 'Avg Price ($)': 1, 'Categories': 'Fast Food', 'Address*': '2349 E 1st St', 'Phone number*': '(912) 538-0711', 'businessUrl': '/biz/chick-fil-a-vidalia-2', 'bizId': 'WDePFsilZSLEhDW7_lUqwQ', 'Sub address': []}, {'Restaurant Name': 'Orale Tacos', 'Avg Price ($)': 1, 'Categories': 'New Mexican Cuisine, Mexican, Desserts', 'Address*': '111 W Congress St', 'Phone number*': '(912) 349-6012', 'businessUrl': '/biz/orale-tacos-savannah', 'bizId': '5Z1h1n-fCrXHEx3Yjq1xrg', 'Sub address': []}, {'Restaurant Name': 'Red Lobster', 'Avg Price ($)': 2, 'Categories': 'Seafood, American (Traditional)', 'Address*': '6550 Tara Blvd', 'Phone number*': '(770) 968-8910', 'businessUrl': '/biz/red-lobster-jonesboro', 'bizId': 'XinnQqra_4I4Fz7y-hbVAQ', 'Sub address': []}, {'Restaurant Name': 'Popeyes Louisiana Kitchen', 'Avg Price ($)': 1, 'Categories': 'Fast Food, Chicken Wings', 'Address*': '5329 Wendy Bagwell Pkwy', 'Phone number*': '(678) 335-6985', 'businessUrl': '/biz/popeyes-louisiana-kitchen-hiram-2', 'bizId': 'LKmNSN2StxwAs7MmDoHEwg', 'Sub address': []}, {'Restaurant Name': 'Marcos Authentic Mexican Food', 'Avg Price ($)': '', 'Categories': 'Mexican', 'Address*': '1435 Hwy 133', 'Phone number*': '(229) 740-1330', 'businessUrl': '/biz/marcos-authentic-mexican-food-moultrie', 'bizId': 'HoaTxXHoIfNQSmGwwsQsww', 'Sub address': []}, {'Restaurant Name': 'Sonic Drive-In', 'Avg Price ($)': '', 'Categories': 'Fast Food, Ice Cream & Frozen Yogurt, Burgers', 'Address*': '1201 Glenwood Ave', 'Phone number*': '(706) 217-1180', 'businessUrl': '/biz/sonic-drive-in-dalton-4', 'bizId': 'JZ04dwIorRia297eUilpBw', 'Sub address': []}]
        
        keys_list = ['Restaurant Name', 'Avg Price ($)', 'Categories', 'Address*', 'Phone number*', 'businessUrl', 'bizId', 'Sub address', 'Business Name (props)', 'Website', 'BizId (props)', 'Menu Link', 'Phone Number', 'Full Address', 'Address Line', 'Address', 'Zipcode', 'City', 'State', 'Working Hours', 'Claimed?', 'Yelp URL', 'Website URL', 'Menu URL']
        with open('detailed_rests_data.csv', 'w') as f:  
            w = csv.DictWriter(f, keys_list)
            w.writeheader()

        
        for business in unique_businesses: 

            bizId = business['bizId']
            url = 'https://www.yelp.com/biz/{}/props'.format(bizId)
            
            yield scrapy.Request(url=url, callback=self.parse, meta={'business': business}) 



    def parse(self, response):

        business  = response.meta.get('business')
        
        response = json.loads(response.text)
        business['Business Name (props)'] = response['bizDetailsPageProps']['businessName']
        if response['bizDetailsPageProps']['bizContactInfoProps']['businessWebsite'] != None:
            business['Website'] = response['bizDetailsPageProps']['bizContactInfoProps']['businessWebsite']['href']
        else:
            business['Website'] = None
        if business['Website'] != None:
            business['Website URL'] = urllib.parse.unquote(business['Website'].split('/biz_redir?url=')[1].split('&')[0])
        business['BizId (props)'] = response['bizDetailsPageProps']['bizContactInfoProps']['businessId']
        if business['bizId'] != business['BizId (props)']:
            print('error', business['bizId'])
        if response['bizDetailsPageProps']['bizContactInfoProps']['businessMenuProps'] != None:
            business['Menu Link'] = response['bizDetailsPageProps']['bizContactInfoProps']['businessMenuProps']['menuLink']['href']
        else:
            business['Menu Link'] = None
        try:
            if business['Menu Link'] != None:
                business ['Menu URL'] = urllib.parse.unquote(business['Menu Link'].split('&url=')[1].split('&')[0])
        except IndexError:
            business ['Menu URL'] = 'https://www.yelp.com' + business['Menu Link']
        business['Phone Number'] = response['bizDetailsPageProps']['bizContactInfoProps']['phoneNumber']
        business['Full Address'] = response['bizDetailsPageProps']['bizContactInfoProps']['businessAddress']
        business['Address Line'] = response['bizDetailsPageProps']['mapBoxProps']['addressProps']['addressLines']
        business['Address'] = business['Address Line'][0]
        if len(business['Address Line'][-1].split(', GA ')) == 2:
            business['Zipcode'] = business['Address Line'][-1].split(', GA ')[-1]
            business['City'] = business['Address Line'][-1].split(', GA ')[0]
            business['State'] = 'GA'
        else:
            print(business['bizId'], business['Address Line'])
        business['Working Hours'] = response['bizDetailsPageProps']['bizHoursProps']['hoursInfoRows']

        if response['gaConfig']['dimensions']['www']['biz_claimed'][1] == 'True':
            business['Claimed?'] = 'Yes'
        elif response['gaConfig']['dimensions']['www']['biz_claimed'][1] == 'False':
            business['Claimed?'] = 'No'
        else:
            print(business['bizId'], response['gaConfig']['dimensions']['www']['biz_claimed'][1])
        business['Yelp URL'] = 'https://www.yelp.com/' + business['businessUrl']
        
        

        keys_list = ['Restaurant Name', 'Avg Price ($)', 'Categories', 'Address*', 'Phone number*', 'businessUrl', 'bizId', 'Sub address', 'Business Name (props)', 'Website', 'BizId (props)', 'Menu Link', 'Phone Number', 'Full Address', 'Address Line', 'Address', 'Zipcode', 'City', 'State', 'Working Hours', 'Claimed?', 'Yelp URL', 'Website URL', 'Menu URL']
        with open('detailed_rests_data.csv', 'a') as f:  
            w = csv.DictWriter(f, keys_list)
            w.writerow(business)




