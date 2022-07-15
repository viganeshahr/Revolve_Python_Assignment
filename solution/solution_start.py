import argparse
import pandas as pd
import os 
import ndjson
import json



def get_params():
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="../input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="../input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="../input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="../output_data/")
    return parser.parse_args()


#*********************  EXTRACTING DATA *********************
def read_file(filename):
    if filename.endswith(".csv"):
        data = pd.read_csv(filename)
        data = data.dropna()
    elif filename.endswith("transactions/"):
        dates = os.listdir(filename) 
        data = []
        for i in dates:
            filepath = filename+i+"/transactions.json"
            transaction_list = pd.read_json(filepath,lines = True)
            transaction_list = transaction_list.dropna()
            data.append(transaction_list)
        data = pd.concat(data) 
        
    return data


#*********************  TRANSFORMING DATA  *********************

def merge_dataframes(data_frame1,data_frame2,condition):
    merged_df = pd.merge(data_frame1,data_frame2,on=condition) 
    return merged_df

def normalize_customer_products(data_frame,column_name):
    exploded_df = data_frame.explode(column_name)
    extracted_df = pd.concat([exploded_df,exploded_df.basket.apply(pd.Series)],axis=1)  # Extracted column values of product_id and their price seperately
    extracted_df = extracted_df.drop(column_name,axis=1)
    return extracted_df

#*********************  LOADING DATA  *************************

def write_to_json(data_frame,location_with_filename):
    data_frame = data_frame.to_dict(orient="records")
    #with open(location_with_filename,'w') as f:
    #    ndjson.dump(data_frame,f,indent=4)
    #print("OUTPUT GENERATED\n")   
    with open(location_with_filename,'w') as f:
        ndjson.dump(data_frame,f) 
    print("OUTPUT GENERATED\n")   


def main():
    params = get_params()
    customers = read_file(params.customers_location) #reads customer data
    products = read_file(params.products_location) #reads products data
    transactions = read_file(params.transactions_location) #reads transactions data
    
    print("Extraction COMPLETED\n")   
    
    customer_transactions = merge_dataframes(customers,transactions,"customer_id")  #merging customer and transactions frame on customer_id

    extracted_frame = normalize_customer_products(customer_transactions,'basket') #extracting frame after normalizing multiple basket values into each row
    
    groupby_frame = extracted_frame.groupby(['customer_id','loyalty_score','product_id'])['product_id'].count().reset_index(name='purchase_count') #taking count of each product for each customer from their s 

    product_merge_frame = merge_dataframes(groupby_frame,products[['product_id','product_category']],"product_id")
    #print(product_merge_frame)
    final_frame = product_merge_frame[['customer_id','loyalty_score','product_id','product_category','purchase_count']]
    final_frame = final_frame.sort_values(by=["customer_id","product_id"])
    
    #mysql = lambda q: sqldf(q,globals())
    #frame1 = mysql("select a.customer_id,a.loyalty_score,a.product_id, count(1) as product_count from df2 a group by a.customer_id,a.loyalty_score,a.product_id")
    #print(frame1)
    #frame2  = mysql("select b.product_category,a.product_count from  products b ")
    #select cust_id, prod_id, count(*) as purchase_count from table group by (cust_id,prod_id)
    
    output_location = params.output_location+"output.json"

    print("Transformation COMPLETED\n")
    write_to_json(final_frame,output_location)
    print("ETL LOAD COMPLETED SUCCESSFULLY")     

    

if __name__ == "__main__":
    main()
