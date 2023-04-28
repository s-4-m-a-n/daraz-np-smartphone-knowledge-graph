from src.components import (scraper, data_transformer, 
                            knowledge_graph_builder)
from src.logger import logging


def pipeline(components:list):
    #run the pipeline with the provided components
    if 'scraper' in components:
        logging.info("running component -> scraper ")
        scraper.run()
        
    
    if 'transformer' in components:
        logging.info("running component -> data_transformer ")
        data_transformer.run()

    
    if 'kg_builder' in components:
        logging.info("running component -> knowledge graph builder")
        knowledge_graph_builder.run()


if __name__ == "__main__":
    components = ['scraper', 'transformer', 'kg_builder']
    pipeline(components)