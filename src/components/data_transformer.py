from src.logger import logging
import src.utils as utils
import pandas as pd
import re
from tqdm import tqdm
import os


COLUMNS=['name','rating', 'review','url', 'brand_id',
         'seller_name', 'variant_hash', 'color_family', 'RAM Memory', 
         'Storage Capacity', 'price']

DETAIL_FIELD = ['Brand', 'Number Of Cameras','Model Year', 
                'Wireless Charging','SIM Type','Battery Capacity', 
                'Camera Front (Megapixels)',]

# defining mapper that map devanagari to english script
eng_to_np = {'Color Family': 'रंग परिवार', 
             'RAM Memory': 'र्याम मेमोरी',
             'Storage Capacity': 'भण्डारण क्षमता'}

config = utils.load_json("src/config.json")
ARTIFACT_ROOT_DIR = config['ARTIFACT_ROOT_DIR']
SCRAPER_ARTIFACT = os.path.join(ARTIFACT_ROOT_DIR, config['SCRAPER_ARTIFACT'])
DATA_TRANS_ARTIFACT = os.path.join(ARTIFACT_ROOT_DIR, config['DATA_TRANS_ARTIFACT'])


def extract_product_name(raw_name):
    '''
        it will return the actual product name by clipping it from the raw_text
        
        args:
            raw_name: string
        returns:
            clipped_name: string
        
        example:
        >> raw_name = "realme 10 Pro Plus | 8GB RAM & 128GB ROM"
        >> extract_product_name(raw_name)
        "realme 10 Pro Plus"
        
    '''
    pattern  = "([\w\s]*)"
    matches = re.search(pattern, raw_name)
    clipped_name = matches.groups(1)[0].strip()
    if len(clipped_name) > 20:
        return clipped_name[:20]
    return clipped_name


def create_dataframe(json_data, columns, detail_field, eng_to_np):
    # create empty dataframe
    df = pd.DataFrame(columns=columns+detail_field)
    # Transforme the JSON into dataframe
    variant_ID = 0
    for product_dict in tqdm(json_data):
        product = {}
        
        for column in columns:
            if column == 'name':
                product[column] = extract_product_name(product_dict[column])
            else:
                product[column] = product_dict.get(column, 'Not Specified')
     
        for column in detail_field:
            product[column] = product_dict['details'].get(column, 'Not Specified')
        
        for price_n_prop in product_dict['details'].get('price_n_prop', []):
            color_family = price_n_prop['props'].get('Color Family', '')
            RAM = price_n_prop['props'].get('RAM Memory', '')
            storage = price_n_prop['props'].get('Storage Capacity', '')

            if color_family == '' and RAM =='' and  storage == '':
                color_family = price_n_prop['props'].get(eng_to_np['Color Family'], 'Not Specified')
                RAM = price_n_prop['props'].get(eng_to_np['RAM Memory'], 'Not Specified')
                storage = price_n_prop['props'].get(eng_to_np['Storage Capacity'], 'Not Specified')

            price_text = price_n_prop['price_info']['salePrice']['text']
            price_value = price_n_prop['price_info']['salePrice']['value']
            
            variant_display_name = f'variant: {variant_ID}' 

            # append row
            product.update({'price': price_text,
                            'price_value': price_value,
                            'color_family': color_family, 
                            'RAM Memory': RAM,
                            'Storage Capacity': storage,
                            'variant_hash': variant_display_name})
            series = pd.Series(product)
            
            
            df = df.append(series, ignore_index=True)
            variant_ID += 1
    return df


def run():
    # load data
    logging.info("loading scraped data")
    json_data = utils.load_json(SCRAPER_ARTIFACT)

    # transform json to dataframe
    logging.info("transorming json into a dataframe")
    df = create_dataframe(json_data, COLUMNS, DETAIL_FIELD, eng_to_np)

    # save dataframe
    logging.info("saving transformed dataframe ")
    df.to_csv(DATA_TRANS_ARTIFACT)


if __name__ == "__main__":
    run()
