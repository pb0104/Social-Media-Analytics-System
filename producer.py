

# def generate_synthetic_order():
#     """Generates synthetic e-commerce order data."""
#     categories = ["Soccer", "Basketball", "Running", "Swimming", "Cycling", "Volleyball", "Tennis"]
#     statuses = ["Processing", "Completed", "Cancelled"]
#     cities = ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Salvador", "Recife", "Fortaleza"]
#     payment_methods = ["Credit Card", "Invoice", "PIX", "Debit Card", "PayPal"]
#     discounts = [0, 0.05, 0.10, 0.15]

#     category = random.choice(categories)
#     status = random.choice(statuses)
#     city = random.choice(cities)
#     payment_method = random.choice(payment_methods)
#     discount = random.choice(discounts)

#     gross_value = fake.pyfloat(min_value=50, max_value=500, right_digits=2)
#     net_value = gross_value * (1 - discount)

#     return {
#         "order_id": str(uuid.uuid4())[:8],
#         "status": status,
#         "category": category,
#         "value": round(net_value, 2),
#         "timestamp": datetime.now().isoformat(),
#         "city": city,
#         "payment_method": payment_method,
#         "discount": round(discount, 2),
#     }

# def run_producer():
#     """Kafka producer that sends synthetic orders to the 'orders' topic."""
#     try:
#         print("[Producer] Connecting to Kafka at localhost:9092...")
#         producer = KafkaProducer(
#             bootstrap_servers="localhost:9092",
#             value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#             request_timeout_ms=30000,
#             max_block_ms=60000,
#             retries=5,
#         )
#         print("[Producer] ✓ Connected to Kafka successfully!")

#         count = 0
#         while True:
#             order = generate_synthetic_order()
#             print(f"[Producer] Sending order #{count}: {order}")

#             future = producer.send("orders", value=order)
#             record_metadata = future.get(timeout=10)
#             print(f"[Producer] ✓ Sent to partition {record_metadata.partition} at offset {record_metadata.offset}")

#             producer.flush()
#             count += 1

#             sleep_time = random.uniform(0.5, 2.0)
#             time.sleep(sleep_time)

#     except Exception as e:
#         print(f"[Producer ERROR] {e}")
#         import traceback
#         traceback.print_exc()
#         raise

# if __name__ == "__main__":
#     run_producer()

import time
import json
import uuid
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

def generate_social_post():
    """Generates synthetic social media post data with engagement metrics."""
    
    # Platform configurations
    platforms = ["Twitter", "Instagram", "Facebook", "LinkedIn", "TikTok", "YouTube"]
    
    post_types = {
        "Twitter": ["tweet", "retweet", "reply", "quote"],
        "Instagram": ["post", "story", "reel", "carousel"],
        "Facebook": ["post", "share", "story", "video"],
        "LinkedIn": ["post", "article", "share", "poll"],
        "TikTok": ["video", "duet", "stitch"],
        "YouTube": ["video", "short", "community_post"]
    }
    
    content_types = ["text", "image", "video", "link", "poll", "carousel"]
    
    # Sentiment analysis
    sentiments = ["positive", "neutral", "negative"]
    sentiment_weights = [0.5, 0.35, 0.15]  # More positive content
    
    # Topics and hashtags
    topics = [
        ("Technology", ["#AI", "#Tech", "#Innovation", "#Coding", "#Startup"]),
        ("Sports", ["#Sports", "#Fitness", "#NBA", "#Soccer", "#Training"]),
        ("Food", ["#Foodie", "#Cooking", "#Recipe", "#Restaurant", "#Vegan"]),
        ("Travel", ["#Travel", "#Wanderlust", "#Adventure", "#Vacation", "#Explore"]),
        ("Fashion", ["#Fashion", "#Style", "#OOTD", "#Streetwear", "#Luxury"]),
        ("Business", ["#Business", "#Entrepreneur", "#Marketing", "#Sales", "#Leadership"]),
        ("Entertainment", ["#Movies", "#Music", "#Gaming", "#Netflix", "#Concert"]),
        ("Health", ["#Health", "#Wellness", "#Fitness", "#Yoga", "#Mental Health"])
    ]
    
    # City/location data
    cities = [
        "New York", "Los Angeles", "Chicago", "London", "Paris", 
        "Tokyo", "Singapore", "Dubai", "Toronto", "Sydney",
        "Berlin", "Mumbai", "São Paulo", "Mexico City", "Seoul"
    ]
    
    # Generate post details
    platform = random.choice(platforms)
    post_type = random.choice(post_types[platform])
    content_type = random.choice(content_types)
    topic, hashtag_pool = random.choice(topics)
    sentiment = random.choices(sentiments, weights=sentiment_weights)[0]
    location = random.choice(cities) if random.random() > 0.3 else None
    
    # Generate hashtags (1-5 hashtags)
    num_hashtags = random.randint(1, 5)
    hashtags = random.sample(hashtag_pool, min(num_hashtags, len(hashtag_pool)))
    
    # Engagement metrics (vary by platform popularity)
    platform_multipliers = {
        "Twitter": 1.0,
        "Instagram": 1.5,
        "Facebook": 1.2,
        "LinkedIn": 0.8,
        "TikTok": 2.0,
        "YouTube": 1.3
    }
    
    multiplier = platform_multipliers[platform]
    
    # Base engagement (can go viral)
    is_viral = random.random() > 0.95  # 5% chance of viral content
    viral_boost = random.uniform(50, 500) if is_viral else 1
    
    base_engagement = random.uniform(10, 1000) * multiplier * viral_boost
    
    likes = int(base_engagement * random.uniform(0.8, 1.2))
    shares = int(base_engagement * random.uniform(0.1, 0.3))
    comments = int(base_engagement * random.uniform(0.05, 0.15))
    views = int(base_engagement * random.uniform(5, 20)) if content_type == "video" else None
    
    # Engagement rate calculation
    total_engagement = likes + shares + comments
    followers = int(random.uniform(100, 100000))
    engagement_rate = round((total_engagement / followers) * 100, 2) if followers > 0 else 0
    
    # Generate timestamp (posts from last 2 hours)
    post_time = datetime.now() - timedelta(minutes=random.randint(0, 120))
    
    # Influencer status (accounts with high followers)
    is_influencer = followers > 50000
    
    # Verified account (20% chance)
    is_verified = random.random() > 0.8
    
    return {
        "post_id": str(uuid.uuid4())[:12],
        "user_id": f"USER_{random.randint(10000, 99999)}",
        "username": fake.user_name(),
        "platform": platform,
        "post_type": post_type,
        "content_type": content_type,
        "topic": topic,
        "hashtags": hashtags,
        "sentiment": sentiment,
        "location": location,
        "likes": likes,
        "shares": shares,
        "comments": comments,
        "views": views,
        "engagement_rate": engagement_rate,
        "followers": followers,
        "is_verified": is_verified,
        "is_influencer": is_influencer,
        "is_viral": is_viral,
        "timestamp": post_time.isoformat(),
        "language": "en"
    }

def run_producer():
    """Kafka producer that sends synthetic social media posts to the 'social_media' topic."""
    try:
        print("[Social Media Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Social Media Producer] ✓ Connected to Kafka successfully!")
        print("[Social Media Producer] 📱 Starting to generate social media posts...\n")

        count = 0
        while True:
            post = generate_social_post()
            
            # Visual indicators
            viral_icon = "🔥" if post["is_viral"] else ""
            verified_icon = "✓" if post["is_verified"] else ""
            sentiment_icons = {"positive": "😊", "neutral": "😐", "negative": "😞"}
            sentiment_icon = sentiment_icons[post["sentiment"]]
            
            print(f"[Producer] {viral_icon}{verified_icon}{sentiment_icon} Post #{count}: "
                  f"{post['platform']} | {post['post_type']} | {post['topic']} | "
                  f"👍{post['likes']} 💬{post['comments']} 🔄{post['shares']} | "
                  f"{' '.join(post['hashtags'][:2])}")

            future = producer.send("social_media", value=post)
            record_metadata = future.get(timeout=10)
            
            if count % 20 == 0 and count > 0:
                print(f"[Producer] ✓ Sent to partition {record_metadata.partition} at offset {record_metadata.offset}")

            producer.flush()
            count += 1

            # Varying post frequency (0.2 to 2 seconds)
            sleep_time = random.uniform(0.2, 2.0)
            time.sleep(sleep_time)

    except Exception as e:
        print(f"[Social Media Producer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_producer()