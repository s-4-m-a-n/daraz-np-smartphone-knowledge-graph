import src.utils as utils
import os

URL = os.getenv("URL")
AUTH = (os.getenv("AUTH_USER"), os.getenv("AUTH_PASS"))


def execute_query(query, driver):
    '''
        execute the query and return the result
        
        args:
            query: string; query_string
            driver: neo4j._sync.driver.BoltDriver
        returns:
            result: list
    '''
    with driver.session() as session:
        result = session.run(query)
        return list(result)

def pipeline(query):
    # establish connection to the neo4j    
    driver = utils.establish_db_connection(URL, AUTH)
    return execute_query(query, driver)


if __name__ == "__main__":
    q = "MATCH (n:Product where n.rating > 2) return n"
    print(pipeline(q)[0]['n']['name'])
    
    