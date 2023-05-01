import src.utils as utils
from src.logger import logging
from tqdm import tqdm
import os

URL = os.getenv("URL")
AUTH = (os.getenv("AUTH_USER"), os.getenv("AUTH_PASS"))

config = utils.load_json("src/config.json")
# ARTIFACT_FILE_PATH = "artifacts/processed_data.csv"
ARTIFACT_FILE_PATH = os.path.join(config['ARTIFACT_ROOT_DIR'], config['DATA_TRANS_ARTIFACT'])


def clean_label(label):
    token = label.split("(")
    return token[0].strip().replace(" ", "_")

def query_segment_generator(x, label, node):
    prop = ""
    for k, v in x.items():
        prop += f"{k}: ${node}.{k},"
    prop = prop[:-1]   
    seg = "MERGE ("+node+":"+clean_label(label)+" {"+prop+"})"
    return seg

def generate_cypher(node1, node2, relation):
    return (
        query_segment_generator(node1, node1['type'], "node1")+
        "ON CREATE SET node1.name = $node1.name "+
        query_segment_generator(node2, node2['type'], "node2")+
        "ON CREATE SET node2.name = $node2.name "+
        f"MERGE (node1)-[r:{relation['type']}]->(node2)"
        )

def db_insert(src, trg, relationship, session):
    cypher = generate_cypher(src, trg, relationship)
    session.run(cypher, node1=src, node2=trg, rel=relationship)


def run():
    logging.disable(logging.DEBUG)
    # establish connection to the database
    logging.info("establishing connection to neo4j")
    logging.info(f"login url: {URL}, auth: {AUTH}")
    driver = utils.establish_db_connection(URL, AUTH)
    
    try:
        utils.is_connected(driver)
    except Exception as e:
        logging.error(e)
        logging.info("execution aborted")
        return None
    
    # load csv 
    logging.info("fetching processed data")
    df = utils.load_csv(ARTIFACT_FILE_PATH)

    # loading database
    logging.info("loading processed data to neo4j")

    with driver.session() as session:
        # iterate through each row in the CSV file
        for idx, row in tqdm(df.iterrows()):
            # create RELATION => HAS_BRAND
            src = {'name':row['name'], 'type': 'Product',
                   'rating': row['rating'], 'review': row['review']}
            
            trg = {'name':row['Brand'], 'type':'Brand', 'id':row['brand_id']}
            # create a relationship between the nodes
            relationship = {'type': 'HAS_BRAND'} 
            db_insert(src, trg, relationship, session)
            
            # create RELATION => HAS_SELLER
            trg = {'name': row['seller_name'], 'type':'Seller'}
            relationship = {'type': 'HAS_SELLER'} 
            db_insert(src, trg, relationship, session)
            
            # create  RELATION => HAS_SPECIFICATION
            SPECS = ['Number Of Cameras', 'Wireless Charging', 
                     'SIM Type', 'Battery Capacity', 'Camera Front (Megapixels)']
            
            for spec in SPECS:
                trg = {'name': row[spec], 'type':spec}
                relationship = {'type': 'HAS_SPECIFICATION'}
                db_insert(src, trg, relationship, session)                
                
            # create RELATION => VARIENT
            trg = {'name': 'variant_'+str(idx), 'type':'Variant', 'url':row['url']}
            relationship={'type': 'VARIANT'}
            db_insert(src, trg, relationship, session)
            varient_trg = trg
            
            # create RELATION => MODEL_YEAR
            trg = {'date':row['Model Year'], 'name': row['Model Year'], 'type':'ModelYear'}
            relationship = {'type': 'MODEL_YEAR'}
            db_insert(src, trg, relationship, session)
            
            
            # create RELATION => HAS
            VAR_SPECS = ['color_family', 'RAM Memory', 
                         'Storage Capacity', 'price']
            
            for spec in VAR_SPECS:
                if spec == 'price':
                    trg = {'name': row[spec], 'type':spec, 'value': row['price_value']}
                else:
                    trg = {'name': row[spec], 'type':spec}
                    
                relationship = {'type': 'HAS_SPECIFICATION'}
                db_insert(varient_trg, trg, relationship, session)

    logging.info("successfully loaded knowledge graph")


if __name__ == "__main__":
    run()