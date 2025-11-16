import json
import psycopg2
from kafka import KafkaConsumer

def run_consumer():
    """Consumes social media posts from Kafka and inserts them into PostgreSQL."""
    try:
        print("[Social Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "social_media",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="social-media-consumer-group",
        )
        print("[Social Consumer] ✓ Connected to Kafka successfully!")

        print("[Social Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Social Consumer] ✓ Connected to PostgreSQL successfully!")

        # Create table for social media posts
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS social_posts (
                post_id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(50),
                username VARCHAR(100),
                platform VARCHAR(50),
                post_type VARCHAR(50),
                content_type VARCHAR(50),
                topic VARCHAR(100),
                hashtags TEXT[],
                sentiment VARCHAR(20),
                location VARCHAR(100),
                likes INTEGER,
                shares INTEGER,
                comments INTEGER,
                views INTEGER,
                engagement_rate NUMERIC(8, 2),
                followers INTEGER,
                is_verified BOOLEAN,
                is_influencer BOOLEAN,
                is_viral BOOLEAN,
                timestamp TIMESTAMP,
                language VARCHAR(10)
            );
            """
        )
        
        # Create indexes for faster queries
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_social_timestamp ON social_posts(timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_social_platform ON social_posts(platform);
            CREATE INDEX IF NOT EXISTS idx_social_topic ON social_posts(topic);
            CREATE INDEX IF NOT EXISTS idx_social_sentiment ON social_posts(sentiment);
            CREATE INDEX IF NOT EXISTS idx_social_viral ON social_posts(is_viral) WHERE is_viral = true;
            CREATE INDEX IF NOT EXISTS idx_social_hashtags ON social_posts USING GIN(hashtags);
            """
        )
        
        print("[Social Consumer] ✓ Table 'social_posts' ready with indexes.")
        print("[Social Consumer] 🎧 Listening for social media posts...\n")

        message_count = 0
        for message in consumer:
            try:
                post = message.value

                insert_query = """
                    INSERT INTO social_posts 
                    (post_id, user_id, username, platform, post_type, content_type, 
                     topic, hashtags, sentiment, location, likes, shares, comments, 
                     views, engagement_rate, followers, is_verified, is_influencer, 
                     is_viral, timestamp, language)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (post_id) DO NOTHING;
                """
                cur.execute(
                    insert_query,
                    (
                        post["post_id"],
                        post["user_id"],
                        post["username"],
                        post["platform"],
                        post["post_type"],
                        post["content_type"],
                        post["topic"],
                        post["hashtags"],
                        post["sentiment"],
                        post.get("location"),
                        post["likes"],
                        post["shares"],
                        post["comments"],
                        post.get("views"),
                        post["engagement_rate"],
                        post["followers"],
                        post["is_verified"],
                        post["is_influencer"],
                        post["is_viral"],
                        post["timestamp"],
                        post["language"],
                    ),
                )
                message_count += 1
                
                # Visual indicators
                viral_icon = "🔥" if post["is_viral"] else "✓"
                verified_icon = "✓" if post["is_verified"] else ""
                sentiment_icons = {"positive": "😊", "neutral": "😐", "negative": "😞"}
                sentiment_icon = sentiment_icons.get(post["sentiment"], "")
                
                print(f"[Consumer] {viral_icon}{verified_icon}{sentiment_icon} #{message_count} "
                      f"Inserted {post['post_id']} | {post['platform']} | "
                      f"{post['topic']} | 👍{post['likes']} 💬{post['comments']} | "
                      f"@{post['username']}")

            except Exception as e:
                print(f"[Social Consumer ERROR] Failed to process message: {e}")
                continue

    except Exception as e:
        print(f"[Social Consumer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_consumer()