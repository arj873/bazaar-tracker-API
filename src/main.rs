use std::fmt::Debug;
use std::thread;
use std::thread::current;
use sqlx::{Connection, postgres::PgPoolOptions, Executor, Row, FromRow, PgPool};
use reqwest::{self};
use serde_json::{Value};
use chrono::{Utc, NaiveDateTime,DateTime};
use tokio::time::{Duration};
use warp::Filter;
use local_ip_address::local_ip;
use warp::reply::Reply;
use warp::http::Response;

#[derive(serde::Serialize, FromRow, Debug)]
pub struct Items {
    pub id: i32,
    pub item_id: std::string::String,
    pub is_del: bool,
}

#[derive(serde::Serialize, FromRow, Debug)]
pub struct QuickInfo {
    pub item_id: std::string::String,
    pub time: DateTime<Utc>,
    pub sell_price: f64,
    pub sell_volume: f64,
    pub sell_moving_week: f64,
    pub sell_orders: f64,
    pub buy_price: f64,
    pub buy_volume: f64,
    pub buy_moving_week: f64,
    pub buy_orders: f64,
}


#[tokio::main]
async fn main(){
    println!("Hello, world!");
    make_new_time_thing().await;
    let tsk1 = async { scrape().await };
    let tsk2 = async { wrp().await };
    tokio::join!(tsk1, tsk2);
}


async fn wrp() {
    let my_local_ip = local_ip();

    if let Ok(my_local_ip) = my_local_ip {
        println!("This is my local IP address: {:?}", my_local_ip);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect("postgres://user:postgres123@localhost:5436/bazz")
            .await
            .unwrap();
        let hello_world = warp::path::end().map(|| "Hello, World at root!");

        let get_items_route = warp::path("items")
            .and(warp::get())
            .and(warp::any().map(move || pool.clone()))
            .and_then(get_items);

        let get_item_curr_price = warp::path("item")
            .and(warp::path::param::<String>())
            .and(warp::get())
            .and_then(get_curr_price);

        let routes = warp::get().and(
            hello_world
                    .or(get_items_route)
                    .or(get_item_curr_price)
        );
        let tsk1 = async { warp::serve(routes.clone())
            .run(([127, 0, 0, 1], 3031))
            .await};
        let tsk2 = async { warp::serve(routes.clone())
            .run((my_local_ip, 3031))
            .await};
        tokio::join!(tsk1, tsk2);

    } else {
        println!("Error getting local IP: {:?}", my_local_ip);
    }


}



async fn get_curr_price(item_id: std::string::String) -> Result<impl warp::Reply, warp::Rejection> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://user:postgres123@localhost:5436/bazz")
        .await
        .unwrap();
    println!("get items234234");
    println!("{}", item_id);
    let items = sqlx::query("SELECT item_id, time::TIMESTAMPTZ, sell_price, sell_volume, sell_moving_week, sell_orders, buy_price, buy_volume, buy_moving_week, buy_orders FROM qick_info \
        WHERE item_id = $1 \
        ORDER BY time DESC \
        LIMIT 1")
        .bind(item_id)
        .fetch_all(&pool)
        .await
        .map_err(|e| {
            eprintln!("Database error: {}", e);
            warp::reject()
        })?;

    let items2: Vec<QuickInfo> = items
        .into_iter()

        .map(|row| QuickInfo {
            item_id: row.get("item_id"),
            time: row.get("time"),
            sell_price: row.get("sell_price"),
            sell_volume: row.get("sell_volume"),
            sell_moving_week: row.get("sell_moving_week"),
            sell_orders: row.get("sell_orders"),
            buy_price: row.get("buy_price"),
            buy_volume: row.get("buy_volume"),
            buy_moving_week: row.get("buy_moving_week"),
            buy_orders: row.get("buy_orders")
        })
        .collect();

    Ok(warp::reply::json(&items2))

}

async fn get_items(pool: PgPool) -> Result<impl warp::Reply, warp::Rejection> {
    println!("get items");
    let items = sqlx::query("SELECT id, item_id, is_delisted FROM items")
        .fetch_all(&pool)
        .await
        .map_err(|e| {
            eprintln!("Database error: {}", e);
            warp::reject()
        })?;
    let items2: Vec<Items> = items
        .into_iter()

        .map(|row| Items {
            id: row.get("id"),
            item_id: row.get("item_id"),
            is_del: row.get("is_delisted")
        })
        .collect();

    Ok(warp::reply::json(&items2))
}


async fn scrape(){
    let mut it = 0;
    let start_time2 = std::time::Instant::now();
    loop {
        let start_time = std::time::Instant::now();
        add_all_items().await;
        add_qinfo().await;
        add_sell_info().await;
        add_buy_info().await;
        let elapsed_time = start_time.elapsed();
        println!("Round: {} \nElapsed: {:.2?} \nTotal elapsed: {:.2?}", it, elapsed_time, start_time2.elapsed());
        it += 1;
        let sleep_duration = Duration::from_secs(20).checked_sub(elapsed_time);
        match sleep_duration {
            Some(duration) => thread::sleep(duration),
            None => continue,
        }
    }
}

async fn get_baz_data() -> std::string::String{
    let resp = reqwest::get("https://api.hypixel.net/skyblock/bazaar").await;
    return resp.unwrap().text().await.unwrap();
}


async fn add_all_items(){
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://user:postgres123@localhost:5436/bazz")
        .await
        .unwrap();


    let bzzdata: Value = serde_json::from_str((get_baz_data().await).as_str()).unwrap();
    for  uh in bzzdata["products"].as_object().iter() {
        for (key, value) in uh.iter() {
            let key_var = Some(key).unwrap();
            let value_var = Some(value).unwrap();
            pool.execute(sqlx::query("
                INSERT INTO items (item_id,is_delisted) VALUES ($1,false) ON CONFLICT (item_id) DO NOTHING
            ").bind(key_var)).await.expect("bruh items");

        }
    }
}

async fn add_qinfo(){
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://user:postgres123@localhost:5436/bazz")
        .await
        .unwrap();

    let bzzdata: Value = serde_json::from_str((get_baz_data().await).as_str()).unwrap();

    let mut transaction = pool.begin().await.expect("Failed to begin transaction");

    for uh in bzzdata["products"].as_object().iter() {
        for (key, value) in uh.iter() {
            let value_var = Some(value).unwrap();
            for uh2 in value_var["quick_status"].as_object().iter() {
                let timestamp = bzzdata["lastUpdated"].to_string().parse::<i64>().unwrap();
                let naive = NaiveDateTime::from_timestamp_opt(timestamp / 1000, (timestamp % 1000) as u32 * 1_000_000).unwrap();
                let datetime = DateTime::<Utc>::from_utc(naive, Utc);
                transaction.execute(sqlx::query("
                INSERT INTO qick_info (item_id, time, sell_price, sell_volume, sell_moving_week, sell_orders, buy_price, buy_volume, buy_moving_week, buy_orders) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (item_id, time) DO NOTHING
            ").bind(&value_var["product_id"].as_str()).bind(&datetime).bind(&uh2["sellPrice"].as_f64()).bind(&uh2["sellVolume"].as_f64()).bind(&uh2["sellMovingWeek"].as_f64()).bind(&uh2["sellOrders"].as_f64()).bind(&uh2["buyPrice"].as_f64()).bind(&uh2["buyVolume"].as_f64()).bind(&uh2["buyMovingWeek"].as_f64()).bind(&uh2["buyOrders"].as_f64())).await.expect("bruh2 qinfo");
            }
        }
    }
    transaction.commit().await.expect("Failed to commit transaction");
}

async fn add_buy_info(){
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://user:postgres123@localhost:5436/bazz")
        .await
        .unwrap();

    let bzzdata: Value = serde_json::from_str((get_baz_data().await).as_str()).unwrap();

    for  uh in bzzdata["products"].as_object().iter() {
        for (key, value) in uh.iter() {
            let key_var = Some(key).unwrap();
            let value_var = Some(value).unwrap();
            let timestamp = bzzdata["lastUpdated"].to_string().parse::<i64>().unwrap();
            let naive = NaiveDateTime::from_timestamp_opt(timestamp / 1000, (timestamp % 1000) as u32 * 1_000_000).unwrap();
            let datetime = DateTime::<Utc>::from_utc(naive, Utc);
            let tmp = pool.execute(sqlx::query("
            INSERT INTO buy_summ (item_id, time) VALUES ($1, $2) ON CONFLICT (item_id, time) DO NOTHING
            ").bind(&value_var["product_id"].as_str()).bind(&datetime)).await.expect("bruh2 qinfo");

            let mut transaction = pool.begin().await.expect("Failed to begin transaction");

            for uh2 in value_var["buy_summary"].as_array().unwrap() {
                transaction.execute(sqlx::query("
                    INSERT INTO summ_buy (item_id, time, amount, price_per_unit, orders) VALUES ($1, $2, $3, $4, $5)
                ").bind(&value_var["product_id"].as_str()).bind(&datetime).bind(&uh2["amount"].as_f64()).bind(&uh2["pricePerUnit"].as_f64()).bind(&uh2["orders"].as_f64())).await.expect("bruh2 qinfo");
            }
            transaction.commit().await.expect("Failed to commit transaction");


        }
    }
}

async fn add_sell_info() {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://user:postgres123@localhost:5436/bazz")
        .await
        .unwrap();
    let bzzdata: Value = serde_json::from_str((get_baz_data().await).as_str()).unwrap();

    for uh in bzzdata["products"].as_object().iter() {
        for (key, value) in uh.iter() {
            let key_var = Some(key).unwrap();
            let value_var = Some(value).unwrap();
            let timestamp = bzzdata["lastUpdated"].to_string().parse::<i64>().unwrap();
            let naive = NaiveDateTime::from_timestamp_opt(timestamp / 1000, (timestamp % 1000) as u32 * 1_000_000).unwrap();
            let datetime = DateTime::<Utc>::from_utc(naive, Utc);
            pool.execute(sqlx::query("
                    INSERT INTO sell_summ (item_id, time) VALUES ($1, $2) ON CONFLICT (item_id, time) DO NOTHING
                    ").bind(&value_var["product_id"].as_str()).bind(&datetime)).await.expect("bruh2 qinfo");

            let mut transaction = pool.begin().await.expect("Failed to begin transaction");
            for uh2 in value_var["sell_summary"].as_array().unwrap() {
                transaction.execute(sqlx::query("
                    INSERT INTO summ_sell (item_id, time, amount, price_per_unit, orders) VALUES ($1, $2, $3, $4, $5)
                ").bind(&value_var["product_id"].as_str()).bind(&datetime).bind(&uh2["amount"].as_f64()).bind(&uh2["pricePerUnit"].as_f64()).bind(&uh2["orders"].as_f64())).await.expect("bruh2 qinfo");
            }
            transaction.commit().await.expect("Failed to commit transaction");

            }
        }
    }

async fn make_chart_data(){
    one_min_data().await;
    five_min_data().await;
    thirty_min_data().await;
    one_hour_data().await;
    four_hour_data().await;
    one_day_data().await;
    one_week_data().await;
    one_month_data().await;
}

async fn one_min_data(){

}

async fn five_min_data(){

}

async fn thirty_min_data(){

}

async fn one_hour_data(){

}

async fn four_hour_data(){

}

async fn one_day_data(){

}

async fn one_week_data(){

}

async fn one_month_data(){

}



async fn make_new_time_thing(){
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://user:postgres123@localhost:5436/bazz")
        .await
        .unwrap();

    pool.execute(sqlx::query("
        CREATE TABLE IF NOT EXISTS items(
            id SERIAL NOT NULL,
            item_id TEXT NOT NULL,
            is_delisted BOOLEAN NOT NULL,
            PRIMARY KEY (item_id)
        );
    ")).await.expect("items error");


    pool.execute(sqlx::query("
        CREATE TABLE IF NOT EXISTS qick_info(
            item_id TEXT NOT NULL,
            time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            sell_price FLOAT NOT NULL ,
            sell_volume FLOAT NOT NULL ,
            sell_moving_week FLOAT NOT NULL ,
            sell_orders FLOAT NOT NULL ,
            buy_price FLOAT NOT NULL,
            buy_volume FLOAT NOT NULL ,
            buy_moving_week FLOAT NOT NULL ,
            buy_orders FLOAT NOT NULL,
            PRIMARY KEY (item_id, time),
            CONSTRAINT fk_item FOREIGN KEY (item_id) REFERENCES items (item_id)
        );
    ")).await.expect("qinfo fail");


    pool.execute(sqlx::query("
        CREATE TABLE IF NOT EXISTS buy_summ(
            item_id TEXT NOT NULL,
            time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            PRIMARY KEY (item_id, time),
            CONSTRAINT fk_item_id FOREIGN KEY (item_id) REFERENCES items (item_id)
        );
    ")).await.expect("buy summ error");

    pool.execute(sqlx::query("
        CREATE TABLE IF NOT EXISTS sell_summ(
            item_id TEXT NOT NULL,
            time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            PRIMARY KEY (item_id, time),
            CONSTRAINT fk_item_id FOREIGN KEY (item_id) REFERENCES items (item_id)
        );
    ")).await.expect("sell summ error");

    pool.execute(sqlx::query("
        CREATE TABLE IF NOT EXISTS summ_sell(
            item_id TEXT NOT NULL,
            time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            amount FLOAT NOT NULL ,
            price_per_unit FLOAT NOT NULL ,
            orders FLOAT NOT NULL,
            CONSTRAINT fk_item_id2 FOREIGN KEY (item_id, time) REFERENCES sell_summ (item_id, time)
        );
    ")).await.expect("summery sell");

    pool.execute(sqlx::query("
        CREATE TABLE IF NOT EXISTS summ_buy(
            item_id TEXT NOT NULL,
            time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            amount FLOAT NOT NULL ,
            price_per_unit FLOAT NOT NULL ,
            orders FLOAT NOT NULL,
            CONSTRAINT fk_item_id2 FOREIGN KEY (item_id, time) REFERENCES buy_summ (item_id, time)
        );
    ")).await.expect("summery buy");

    pool.execute(sqlx::query("
        CREATE TABLE IF NOT EXISTS one_min_chart(
            item_id TEXT NOT NULL,
            time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            open_price FLOAT NOT NULL ,
            close_price FLOAT NOT NULL ,
            high_price FLOAT NOT NULL ,
            low_price FLOAT NOT NULL ,
            volume FLOAT NOT NULL ,
            average_price FLOAT NOT NULL,
            PRIMARY KEY (item_id, time),
            CONSTRAINT fk_item FOREIGN KEY (item_id) REFERENCES items (item_id)
        );
    ")).await.expect("1 min chart");

    pool.execute(sqlx::query("
        CREATE TABLE IF NOT EXISTS five_min_chart(
            item_id TEXT NOT NULL,
            time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            open_price FLOAT NOT NULL ,
            close_price FLOAT NOT NULL ,
            high_price FLOAT NOT NULL ,
            low_price FLOAT NOT NULL ,
            volume FLOAT NOT NULL ,
            average_price FLOAT NOT NULL,
            PRIMARY KEY (item_id, time),
            CONSTRAINT fk_item FOREIGN KEY (item_id) REFERENCES items (item_id)
        );
    ")).await.expect("5 min chart");

    pool.execute(sqlx::query("
        CREATE TABLE IF NOT EXISTS thirty_min_chart(
            item_id TEXT NOT NULL,
            time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            open_price FLOAT NOT NULL ,
            close_price FLOAT NOT NULL ,
            high_price FLOAT NOT NULL ,
            low_price FLOAT NOT NULL ,
            volume FLOAT NOT NULL ,
            average_price FLOAT NOT NULL,
            PRIMARY KEY (item_id, time),
            CONSTRAINT fk_item FOREIGN KEY (item_id) REFERENCES items (item_id)
        );
    ")).await.expect("30 min chart");

    pool.execute(sqlx::query("
        CREATE TABLE IF NOT EXISTS one_hour_chart(
            item_id TEXT NOT NULL,
            time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            open_price FLOAT NOT NULL ,
            close_price FLOAT NOT NULL ,
            high_price FLOAT NOT NULL ,
            low_price FLOAT NOT NULL ,
            volume FLOAT NOT NULL ,
            average_price FLOAT NOT NULL,
            PRIMARY KEY (item_id, time),
            CONSTRAINT fk_item FOREIGN KEY (item_id) REFERENCES items (item_id)
        );
    ")).await.expect("1 hour chart");

    pool.execute(sqlx::query("
        CREATE TABLE IF NOT EXISTS four_hour_chart(
            item_id TEXT NOT NULL,
            time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            open_price FLOAT NOT NULL ,
            close_price FLOAT NOT NULL ,
            high_price FLOAT NOT NULL ,
            low_price FLOAT NOT NULL ,
            volume FLOAT NOT NULL ,
            average_price FLOAT NOT NULL,
            PRIMARY KEY (item_id, time),
            CONSTRAINT fk_item FOREIGN KEY (item_id) REFERENCES items (item_id)
        );
    ")).await.expect("4 hour chart");

    pool.execute(sqlx::query("
        CREATE TABLE IF NOT EXISTS one_day_chart(
            item_id TEXT NOT NULL,
            time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            open_price FLOAT NOT NULL ,
            close_price FLOAT NOT NULL ,
            high_price FLOAT NOT NULL ,
            low_price FLOAT NOT NULL ,
            volume FLOAT NOT NULL ,
            average_price FLOAT NOT NULL,
            PRIMARY KEY (item_id, time),
            CONSTRAINT fk_item FOREIGN KEY (item_id) REFERENCES items (item_id)
        );
    ")).await.expect("1 day chart");

    pool.execute(sqlx::query("
        CREATE TABLE IF NOT EXISTS one_week_chart(
            item_id TEXT NOT NULL,
            time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            open_price FLOAT NOT NULL ,
            close_price FLOAT NOT NULL ,
            high_price FLOAT NOT NULL ,
            low_price FLOAT NOT NULL ,
            volume FLOAT NOT NULL ,
            average_price FLOAT NOT NULL,
            PRIMARY KEY (item_id, time),
            CONSTRAINT fk_item FOREIGN KEY (item_id) REFERENCES items (item_id)
        );
    ")).await.expect("1 week chart");

    pool.execute(sqlx::query("
        CREATE TABLE IF NOT EXISTS one_month_chart(
            item_id TEXT NOT NULL,
            time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            open_price FLOAT NOT NULL ,
            close_price FLOAT NOT NULL ,
            high_price FLOAT NOT NULL ,
            low_price FLOAT NOT NULL ,
            volume FLOAT NOT NULL ,
            average_price FLOAT NOT NULL,
            PRIMARY KEY (item_id, time),
            CONSTRAINT fk_item FOREIGN KEY (item_id) REFERENCES items (item_id)
        );
    ")).await.expect("1 month chart");


    pool.execute(sqlx::query("
        CREATE INDEX ON sell_summ (item_id,time DESC);;        
    ")).await.expect("Index making error sell");
    pool.execute(sqlx::query("
        CREATE INDEX ON buy_summ (item_id,time DESC);
    ")).await.expect("Index making error buy");
    pool.execute(sqlx::query("
        CREATE INDEX ON qick_info (item_id,time DESC);        
    ")).await.expect("Index making error info");

    pool.execute(sqlx::query("
        CREATE INDEX ON one_min_chart (item_id,time DESC);
    ")).await.expect("Index making error info");
    pool.execute(sqlx::query("
        CREATE INDEX ON five_min_chart (item_id,time DESC);
    ")).await.expect("Index making error info");
    pool.execute(sqlx::query("
        CREATE INDEX ON thirty_min_chart (item_id,time DESC);
    ")).await.expect("Index making error info");
    pool.execute(sqlx::query("
        CREATE INDEX ON one_hour_chart (item_id,time DESC);
    ")).await.expect("Index making error info");
    pool.execute(sqlx::query("
        CREATE INDEX ON four_hour_chart (item_id,time DESC);
    ")).await.expect("Index making error info");
    pool.execute(sqlx::query("
        CREATE INDEX ON one_day_chart (item_id,time DESC);
    ")).await.expect("Index making error info");
    pool.execute(sqlx::query("
        CREATE INDEX ON one_week_chart (item_id,time DESC);
    ")).await.expect("Index making error info");
    pool.execute(sqlx::query("
        CREATE INDEX ON one_month_chart (item_id,time DESC);
    ")).await.expect("Index making error info");

}

