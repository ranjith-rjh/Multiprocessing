import pandas as pd
from datetime import datetime
from tqdm import tqdm
import os
import multiprocessing as mp
from multiprocessing import Pool
from functools import partial


# -------------- Problem : foreach creator, associate each product

# Define functions

def CreateMatch(creator, product, list) : 
    list.append([creator, product])

def IterateOverCreator(creator, df_product) :
    list_to_df = []
    df_product.apply(lambda product: CreateMatch(creator, product['id_product'], list_to_df), axis=1)
    df_res = pd.DataFrame(list_to_df, columns=['Creator', 'Product'])
    return df_res
    
    
# Main program

if __name__ == '__main__' : 

    start = datetime.now()

    # -------------- Data importation

    df_creators = pd.read_csv('creators.csv', header=0, sep=';')

    # Define the type before the loading
    dtypes = {
                'id_product' : int,
                'energy_100g' : float,
                'fat_100g' : float,
                'saturated_fat_100g' : float,
                'carbohydrates_100g' : float,
                'sugars_100g' : float,
                'proteins_100g' : float,
                'nutrition_score_fr_100g' : float,
                'energy_kcal_100g' : float,
                'ecoscore_score' :float 
    }

    df_products = pd.read_csv('products.csv', header=0, sep=';', dtype=dtypes)

    # ----------------- Multiprocessing

    p = mp.Pool()
    func_args = partial(IterateOverCreator, df_product=df_products)

    # Creating a list of creators
    creators_name_list = df_creators['Name'].tolist()

    # Result Dataframe
    df_result = pd.DataFrame(columns=['Creator', 'Product'])

    # Adding data to the result dataframe from the function
    with tqdm(total=len(p.map(func_args, creators_name_list))) as pbar : 
        for result in p.map(func_args, creators_name_list) : 
            df_result = pd.concat([df_result, result], ignore_index=True)
            pbar.update()

    df_result.to_csv('output/result.csv', index=False, header=0, sep=';')

    end = datetime.now()

    print('Rows generated : ' + str(len(df_result)))
    print('Program execution time: ' + str(end-start))




