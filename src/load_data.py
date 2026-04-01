import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv


def load_data():
    load_dotenv()

    connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(
        os.getenv('DB_USER'),
        os.getenv('DB_PASSWORD'),
        os.getenv('DB_HOST'),
        os.getenv('DB_PORT'),
        os.getenv('DB_NAME'),
    )
    engine = create_engine(connection_string)

    query = """
    select p.user_id as user_id,
	   p.device_type_canonical as device_type_canonical,
	   p.order_id as order_id,  
	   p.created_dt_msk as order_dt,
	   p.created_ts_msk as order_ts,
	   p.currency_code as currency_code,
	   p.revenue as revenue,
	   p.tickets_count as tickets_count,
	   p.created_dt_msk::date - LAG(p.created_dt_msk::date) over(partition by p.user_id order by p.created_dt_msk) as days_since_prev,
	   p.event_id as event_id,
	   e.event_name_code as event_name,
       e.event_type_main as event_type_main,
	   p.service_name as service_name,
	   r.region_name as region_name,
	   c.city_name as city_name
    from afisha.purchases p 
    join afisha.events e USING(event_id)
    join afisha.city c USING(city_id)
    join afisha.regions r USING(region_id)
    where p.device_type_canonical in ('mobile', 'desktop') and e.event_type_main != ('фильм')
    order by user_id 
    """

    df = pd.read_sql_query(query, con=engine)
    engine.dispose()

    return df


if __name__ == "__main__":
    df = load_data()
    df.to_csv("../data/raw_data.csv", index=False)