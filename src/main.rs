// use std::future::Future;

use serde::de::value;
// use dashmap::DashMap;
use sqlx::{Connection,postgres::PgPoolOptions, Executor, PgConnection, types::uuid::timestamp};
use reqwest::{self, Error};
use serde_json::{Value,Result};
use chrono::{Utc, TimeZone, NaiveDateTime,DateTime};
use substring::Substring;
use tokio::time::{sleep, Duration};
use std::thread;

#[tokio::main]
async fn main(){

    // let pool = PgPoolOptions::new()
    //     .max_connections(5)
    //     .connect("postgres://user:postgres123@localhost:5436/bazz")
    //     .await
    //     .unwrap();



//  Ok(())

    // test().await;
    // println!("{:?}",getBazData().await);
    // println!("{}",getBazData().await);
    make_new_time_thing().await;
    let mut it = 0;
    let start_time2 = std::time::Instant::now();
    loop {
        let start_time = std::time::Instant::now();
        println!("start {}", it);
        addAllItems().await;
        // println!("step 1");
        // let t3 = thread::spawn(|| addQInfo());
        // println!("step 2");
        // let t1 = thread::spawn(|| addBuyInfo());
        // println!("step 3");
        // let t2 = thread::spawn(|| addSellInfo());
        // println!("step 4");
        // t1.join().unwrap().await;
        // t2.join().unwrap().await;
        // t3.join().unwrap().await;
        addSellInfo().await;
        addBuyInfo().await;
        addQInfo().await;
        it += 1;
        println!("done {}", it);

        let elapsed_time = start_time.elapsed();
        println!("Elapsed: {:.2?} \nTotal elapsed: {:.2?}", elapsed_time, start_time2.elapsed());
        // Subtract the elapsed time from the desired interval of 20 seconds
        let sleep_duration = Duration::from_secs(20).checked_sub(elapsed_time);

        match sleep_duration {
            Some(duration) => thread::sleep(duration),
            None => continue, // Execution took longer than 20 seconds, immediately run again
        }
    }

    // make_new_time_thing().await;
    // addAllItems().await;
    // addQInfo().await;
    // addBuyInfo().await;
    // addSellInfo().await;

}





async fn getBazData() -> std::string::String{
    let resp = reqwest::get("https://api.hypixel.net/skyblock/bazaar").await;
    return resp.unwrap().text().await.unwrap();
}


async fn test(){
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://user:postgres123@localhost:5436/bazz")
        .await
        .unwrap();

    let tmp = pool.execute(sqlx::query("
    SELECT * FROM items
    ")).await;
    println!("{:?}", tmp);

}

async fn addAllItems(){
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://user:postgres123@localhost:5436/bazz")
        .await
        .unwrap();


    let bzzdata: Value = serde_json::from_str((getBazData().await).as_str()).unwrap();
    // println!("{}",bzzdata["products"].as);

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

async fn addQInfo(){
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://user:postgres123@localhost:5436/bazz")
        .await
        .unwrap();

    let bzzdata: Value = serde_json::from_str((getBazData().await).as_str()).unwrap();

    // for  uh in bzzdata["products"].as_object().iter() {
    //     for (key, value) in uh.iter() {
    //         let key_var = Some(key).unwrap();
    //         let value_var = Some(value).unwrap();
    //         for uh2 in value_var["quick_status"].as_object().iter() {
    //             let timestamp = bzzdata["lastUpdated"].to_string().parse::<i64>().unwrap();
    //             let naive = NaiveDateTime::from_timestamp_opt(timestamp / 1000, (timestamp % 1000) as u32 * 1_000_000).unwrap();
    //             let datetime = DateTime::<Utc>::from_utc(naive, Utc);
    //             let newdate = datetime.format("%Y-%m-%d %H:%M:%S");
    //             // println!("{}",datetime.to_string());
    //             pool.execute(sqlx::query("
    //                 INSERT INTO qick_info (item_id, time, sellPrice, sellVolume, sellMovingWeek, sellOrders, buyPrice, buyVolume, buyMovingWeek, buyOrders) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    //         ").bind(&value_var["product_id"].as_str()).bind(&datetime).bind(&uh2["sellPrice"].as_f64()).bind(&uh2["sellVolume"].as_f64()).bind(&uh2["sellMovingWeek"].as_f64()).bind(&uh2["sellOrders"].as_f64()).bind(&uh2["buyPrice"].as_f64()).bind(&uh2["buyVolume"].as_f64()).bind(&uh2["buyMovingWeek"].as_f64()).bind(&uh2["buyOrders"].as_f64())).await.expect("bruh2 qinfo");
    //
    //             }
    //     }
    // }

    let mut transaction = pool.begin().await.expect("Failed to begin transaction");

    for uh in bzzdata["products"].as_object().iter() {
        for (key, value) in uh.iter() {
            let value_var = Some(value).unwrap();
            for uh2 in value_var["quick_status"].as_object().iter() {
                let timestamp = bzzdata["lastUpdated"].to_string().parse::<i64>().unwrap();
                let naive = NaiveDateTime::from_timestamp_opt(timestamp / 1000, (timestamp % 1000) as u32 * 1_000_000).unwrap();
                let datetime = DateTime::<Utc>::from_utc(naive, Utc);
                let newdate = datetime.format("%Y-%m-%d %H:%M:%S");
            transaction.execute(sqlx::query("
                INSERT INTO qick_info (item_id, time, sellPrice, sellVolume, sellMovingWeek, sellOrders, buyPrice, buyVolume, buyMovingWeek, buyOrders) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ").bind(&value_var["product_id"].as_str()).bind(&datetime).bind(&uh2["sellPrice"].as_f64()).bind(&uh2["sellVolume"].as_f64()).bind(&uh2["sellMovingWeek"].as_f64()).bind(&uh2["sellOrders"].as_f64()).bind(&uh2["buyPrice"].as_f64()).bind(&uh2["buyVolume"].as_f64()).bind(&uh2["buyMovingWeek"].as_f64()).bind(&uh2["buyOrders"].as_f64())).await.expect("bruh2 qinfo");
            }
        }
    }
    transaction.commit().await.expect("Failed to commit transaction");
}

async fn addBuyInfo(){
    let pool = PgPoolOptions::new()
    .max_connections(5)
    .connect("postgres://user:postgres123@localhost:5436/bazz")
    .await
    .unwrap();

    let bzzdata: Value = serde_json::from_str((getBazData().await).as_str()).unwrap();

    for  uh in bzzdata["products"].as_object().iter() {
        for (key, value) in uh.iter() {
            let key_var = Some(key).unwrap();
            let value_var = Some(value).unwrap();
            let timestamp = bzzdata["lastUpdated"].to_string().parse::<i64>().unwrap();
            let naive = NaiveDateTime::from_timestamp_opt(timestamp / 1000, (timestamp % 1000) as u32 * 1_000_000).unwrap();
            let datetime = DateTime::<Utc>::from_utc(naive, Utc);
            let newdate = datetime.format("%Y-%m-%d %H:%M:%S");
            pool.execute(sqlx::query("
            INSERT INTO buy_summ (item_id, time) VALUES ($1, $2)
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

async fn addSellInfo() {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://user:postgres123@localhost:5436/bazz")
        .await
        .unwrap();
    let bzzdata: Value = serde_json::from_str((getBazData().await).as_str()).unwrap();

    for uh in bzzdata["products"].as_object().iter() {
        for (key, value) in uh.iter() {
            let key_var = Some(key).unwrap();
            let value_var = Some(value).unwrap();
            let timestamp = bzzdata["lastUpdated"].to_string().parse::<i64>().unwrap();
            let naive = NaiveDateTime::from_timestamp_opt(timestamp / 1000, (timestamp % 1000) as u32 * 1_000_000).unwrap();
            let datetime = DateTime::<Utc>::from_utc(naive, Utc);
            let newdate = datetime.format("%Y-%m-%d %H:%M:%S");
            pool.execute(sqlx::query("
                    INSERT INTO sell_summ (item_id, time) VALUES ($1, $2)
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
            sellPrice FLOAT NOT NULL ,
            sellVolume FLOAT NOT NULL ,
            sellMovingWeek FLOAT NOT NULL ,
            sellOrders FLOAT NOT NULL ,
            buyPrice FLOAT NOT NULL,
            buyVolume FLOAT NOT NULL ,
            buyMovingWeek FLOAT NOT NULL ,
            buyOrders FLOAT NOT NULL,
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
        CREATE INDEX ON sell_summ (item_id,time DESC);;        
    ")).await.expect("Index making error sell");
    pool.execute(sqlx::query("
        CREATE INDEX ON buy_summ (item_id,time DESC);
    ")).await.expect("Index making error buy");
    pool.execute(sqlx::query("
        CREATE INDEX ON qick_info (item_id,time DESC);        
    ")).await.expect("Index making error info");
    }

