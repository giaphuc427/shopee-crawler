import pandas as pd 
import requests
from datetime import datetime

def get_json_data_from_request(url, querystring, headers):
    response = requests.request('GET', url= url, headers=headers, params=querystring)
    return response.json()

def get_list_shop(data):
    list_shop_scrape = []
    data_brands = data['data']['brands']
    
    for brand_idx in range(len(data_brands)):
        data_brands_ids = data_brands[brand_idx]['brand_ids']
        for brand_ids_idx in range(len(data_brands_ids)):
            shop_username = data_brands_ids[brand_ids_idx]['username']
            list_shop_scrape.append(shop_username)
    return list_shop_scrape

def get_info_shop(data):
    info_shop = data['data']
    
    shop_name = info_shop['name']
    no_product = info_shop['item_count']
    no_following = info_shop['account']['following_count']
    
    response_time = ''   
    if info_shop['response_time'] == None:
        response_time = ''
    elif info_shop['response_time'] >= 3600:
        response_time = '(in hours)'
    else: response_time = '(in minutes)'

    chat_reponse_rate = str(info_shop['response_rate']) + '% ' + response_time
    no_follower = info_shop['follower_count']
    
    rating_star = info_shop['rating_star']
    rating_count = info_shop['rating_bad'] + info_shop['rating_good'] + info_shop['rating_normal']
    
    if rating_star == None:
        rating_star = 0
    
    rating = f'{rating_star:.1f} ({rating_count} ratings)'
    
    current_time_dt = datetime.now()
    craeted_shop_time_dt = datetime.fromtimestamp(info_shop['ctime'])

    relative_in_month = (current_time_dt - craeted_shop_time_dt).days // 30
    created_time = f'{relative_in_month} months ago'
    
    return {
        'shop_name': shop_name,
        'no_product': no_product,
        'no_following': no_following,
        'chat_reponse_rate': chat_reponse_rate,
        'no_follower': no_follower,
        'rating': rating,
        'created_time': created_time  
    }

def run_shopee_etl():
    url_shop_name = 'https://shopee.vn/api/v4/official_shop/get_shops_by_category'
    querystring_shop_name = {'after':'1632511833','need_zhuyin':'0','category_id':'11035567'}
    headers_shop_name = {
        'cookie': 'REC_T_ID=bb85fda0-1e97-11ec-9fbd-b47af14b9080; SPC_R_T_ID=wSjpQy4X2u0Rk478kqWyyUTduTnJO7DugXwprCWf9gzhzCNY9SD3X9S9lQvo7JaUfAWLUvry8ZwW7Zyurk2A2FSo3ZSInERjApTwuAW10yBspVuYuNPOdRIhl5sRJ5%2FqLOpc%2BZRiHrIYidfnSc2saD7Jpf%2BbsZzzHolOaDoFzsc%3D; SPC_R_T_IV=Y25sWU1Sc3FzelhCWGVReQ%3D%3D; SPC_T_ID=wSjpQy4X2u0Rk478kqWyyUTduTnJO7DugXwprCWf9gzhzCNY9SD3X9S9lQvo7JaUfAWLUvry8ZwW7Zyurk2A2FSo3ZSInERjApTwuAW10yBspVuYuNPOdRIhl5sRJ5%2FqLOpc%2BZRiHrIYidfnSc2saD7Jpf%2BbsZzzHolOaDoFzsc%3D; SPC_T_IV=Y25sWU1Sc3FzelhCWGVReQ%3D%3D',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer BQBqABMFVDGnfWVAbAQOSVurlv0pci4YRDysFs__Pjae4f8BqqrX4gxDrRC7b2rwJXOXA1CyAmStcO0jJkBNctzNDmVSDOHl6GnxgE29ih5tdwdGwjETedrsqFUCHgAqMPAYJz9ZP4Dk6sv4_Qxp'
    }
    
    data_shop_username = get_json_data_from_request(url=url_shop_name, querystring=querystring_shop_name, headers=headers_shop_name)
    list_shop_scrape = get_list_shop(data_shop_username)
    
    url_shop_info = 'https://shopee.vn/api/v4/shop/get_shop_detail'
    headers_shop_info = {
        'cookie': 'REC_T_ID=bb85fda0-1e97-11ec-9fbd-b47af14b9080; SPC_R_T_ID=wSjpQy4X2u0Rk478kqWyyUTduTnJO7DugXwprCWf9gzhzCNY9SD3X9S9lQvo7JaUfAWLUvry8ZwW7Zyurk2A2FSo3ZSInERjApTwuAW10yBspVuYuNPOdRIhl5sRJ5%2FqLOpc%2BZRiHrIYidfnSc2saD7Jpf%2BbsZzzHolOaDoFzsc%3D; SPC_R_T_IV=Y25sWU1Sc3FzelhCWGVReQ%3D%3D; SPC_T_ID=wSjpQy4X2u0Rk478kqWyyUTduTnJO7DugXwprCWf9gzhzCNY9SD3X9S9lQvo7JaUfAWLUvry8ZwW7Zyurk2A2FSo3ZSInERjApTwuAW10yBspVuYuNPOdRIhl5sRJ5%2FqLOpc%2BZRiHrIYidfnSc2saD7Jpf%2BbsZzzHolOaDoFzsc%3D; SPC_T_IV=Y25sWU1Sc3FzelhCWGVReQ%3D%3D',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer BQBqABMFVDGnfWVAbAQOSVurlv0pci4YRDysFs__Pjae4f8BqqrX4gxDrRC7b2rwJXOXA1CyAmStcO0jJkBNctzNDmVSDOHl6GnxgE29ih5tdwdGwjETedrsqFUCHgAqMPAYJz9ZP4Dk6sv4_Qxp'
    }
    
    shop_name, no_product, no_following, chat_reponse_rate, no_follower, rating, created_time = [], [], [], [], [], [], []
    for shop_idx in range(len(list_shop_scrape)):
        querystring_shop_info = {'after':'1632511833','sort_sold_out':'0','username':list_shop_scrape[shop_idx]}
        
        data_shop_info = get_json_data_from_request(url=url_shop_info, querystring=querystring_shop_info, headers=headers_shop_info)
        info_shop = get_info_shop(data_shop_info)
        
        shop_name.append(info_shop['shop_name'])
        no_product.append(info_shop['no_product'])
        no_following.append(info_shop['no_following'])
        chat_reponse_rate.append(info_shop['chat_reponse_rate'])
        no_follower.append(info_shop['no_follower'])
        rating.append(info_shop['rating'])
        created_time.append(info_shop['created_time'])
    
    shop_info_dict = {
        'shop_name' : shop_name,
        'shop_username': list_shop_scrape,
        'no_product': no_product,
        'no_following' : no_following,
        'chat_reponse_rate' : chat_reponse_rate,
        'no_follower': no_follower,
        'rating' : rating,
        'created_time' : created_time
    }
    shop_info_dict_df = pd.DataFrame(shop_info_dict, columns = ['shop_name', 'shop_username', 'no_product', 'no_following', 'chat_reponse_rate', 'no_follower', 'rating', 'created_time'])
    
    shop_info_dict_df.to_csv('./data/shopee-brands.xlsx', index=False)