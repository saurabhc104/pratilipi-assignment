from django.shortcuts import render
from google.cloud import bigquery
from django.http import HttpResponse, JsonResponse
import time
from datetime import datetime
from dateutil.relativedelta import *


client = bigquery.Client()


def index(request):
    st = time.time()
    user_id = request.GET.get('user_id')
    query_date = request.GET.get('query_date')
    lower_bound_date = (datetime.strptime(query_date, '%Y-%m-%d') - relativedelta(weeks=2)).strftime('%Y-%m-%d')

    #query = "SELECT * FROM `pratilipi-de-assignment.user_interaction.user_interaction_partitioned` WHERE updated_at > \"2021-12-10\" and user_id=6504233704206137 LIMIT 10"
    # query = "SELECT * FROM `pratilipi-de-assignment.content_data.content_meta` LIMIT 100"
    query  = """
        select ARRAY_AGG(struct(category_name, category_count)) as json_val from (
            select user_id, category_name, count(*) as category_count from (
                    SELECT A.*, B.category_name from (
                        SELECT * FROM `pratilipi-de-assignment.user_interaction.user_interaction_partitioned` 
                        WHERE updated_at >= '{lower_bound_date}' and updated_at<='{query_date}' and user_id={user_id} and read_percent>90
                    ) as A
                    inner join 
                    (
                        SELECT * FROM `pratilipi-de-assignment.content_data.content_meta` 
                    ) as B on A.content_id=B.content_id 
            ) group by user_id, category_name
        ) group by user_id
        
    """.format(query_date=query_date, user_id=user_id, lower_bound_date=lower_bound_date)
    print(query)
    query_job = client.query(query)
    
    total_sum = 0
    response_dict = dict()
    for row in query_job:
        response_dict = dict()
        for i, val in row.items():
            for category in val:
                total_sum = total_sum + category.get('category_count')
                response_dict[category.get('category_name')] = category.get('category_count')


    response_dict = {i:val/total_sum for i, val in response_dict.items()} 
    print(time.time() - st)
    return JsonResponse(response_dict)




