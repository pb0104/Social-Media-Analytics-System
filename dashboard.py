import time
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Social Media Analytics Dashboard", layout="wide", page_icon="📱")

# Custom CSS
st.markdown("""
    <style>
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
    }
    .viral-post {
        background-color: #fff3cd;
        padding: 10px;
        border-radius: 5px;
        border-left: 4px solid #ff6b6b;
    }
    </style>
""", unsafe_allow_html=True)

st.title("📱 Real-Time Social Media Analytics Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"

@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)

engine = get_engine(DATABASE_URL)

def load_data(platform_filter: str | None = None, 
              sentiment_filter: str | None = None,
              topic_filter: str | None = None,
              limit: int = 1000,
              viral_only: bool = False) -> pd.DataFrame:
    """Load social media posts with optional filters."""
    base_query = "SELECT * FROM social_posts WHERE 1=1"
    params = {}
    
    if viral_only:
        base_query += " AND is_viral = true"
    
    if platform_filter and platform_filter != "All Platforms":
        base_query += " AND platform = :platform"
        params["platform"] = platform_filter
    
    if sentiment_filter and sentiment_filter != "All Sentiments":
        base_query += " AND sentiment = :sentiment"
        params["sentiment"] = sentiment_filter
    
    if topic_filter and topic_filter != "All Topics":
        base_query += " AND topic = :topic"
        params["topic"] = topic_filter
    
    base_query += " ORDER BY timestamp DESC LIMIT :limit"
    params["limit"] = limit

    try:
        df = pd.read_sql_query(text(base_query), con=engine.connect(), params=params)
        if not df.empty and "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

def get_filter_options():
    """Get unique values for filters."""
    try:
        with engine.connect() as conn:
            platforms_df = pd.read_sql_query(text("SELECT DISTINCT platform FROM social_posts ORDER BY platform"), conn)
            sentiments_df = pd.read_sql_query(text("SELECT DISTINCT sentiment FROM social_posts ORDER BY sentiment"), conn)
            topics_df = pd.read_sql_query(text("SELECT DISTINCT topic FROM social_posts ORDER BY topic"), conn)
            
            return (
                ["All Platforms"] + platforms_df["platform"].tolist(),
                ["All Sentiments"] + sentiments_df["sentiment"].tolist(),
                ["All Topics"] + topics_df["topic"].tolist()
            )
    except:
        return ["All Platforms"], ["All Sentiments"], ["All Topics"]

# Sidebar controls
st.sidebar.header("⚙️ Dashboard Controls")

platforms, sentiments, topics = get_filter_options()
selected_platform = st.sidebar.selectbox("📱 Platform", platforms)
selected_sentiment = st.sidebar.selectbox("😊 Sentiment", sentiments)
selected_topic = st.sidebar.selectbox("📌 Topic", topics)
show_viral_only = st.sidebar.checkbox("🔥 Show Viral Posts Only", value=False)

st.sidebar.markdown("---")
update_interval = st.sidebar.slider("⏱️ Update Interval (seconds)", min_value=3, max_value=30, value=5)
limit_records = st.sidebar.number_input("📊 Records to Load", min_value=500, max_value=10000, value=1000, step=500)

if st.sidebar.button("🔄 Refresh Now"):
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("### 📈 About")
st.sidebar.info("Real-time analytics tracking social media posts across multiple platforms with sentiment analysis and engagement metrics.")

placeholder = st.empty()

while True:
    df = load_data(
        platform_filter=selected_platform,
        sentiment_filter=selected_sentiment,
        topic_filter=selected_topic,
        limit=int(limit_records),
        viral_only=show_viral_only
    )

    with placeholder.container():
        if df.empty:
            st.warning("⏳ No posts found. Waiting for data...")
            time.sleep(update_interval)
            continue

        # === KEY METRICS ===
        st.markdown("### 📊 Key Performance Indicators")
        
        total_posts = len(df)
        total_likes = df["likes"].sum()
        total_comments = df["comments"].sum()
        total_shares = df["shares"].sum()
        avg_engagement = df["engagement_rate"].mean()
        viral_count = df["is_viral"].sum()
        verified_count = df["is_verified"].sum()
        influencer_count = df["is_influencer"].sum()
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📝 Total Posts", f"{total_posts:,}")
        with col2:
            st.metric("👍 Total Likes", f"{total_likes:,}")
        with col3:
            st.metric("💬 Total Comments", f"{total_comments:,}")
        with col4:
            st.metric("🔄 Total Shares", f"{total_shares:,}")
        
        col5, col6, col7, col8 = st.columns(4)
        with col5:
            st.metric("📈 Avg Engagement", f"{avg_engagement:.2f}%")
        with col6:
            st.metric("🔥 Viral Posts", f"{int(viral_count)}")
        with col7:
            st.metric("✓ Verified Users", f"{int(verified_count)}")
        with col8:
            st.metric("⭐ Influencers", f"{int(influencer_count)}")

        # === VIRAL POSTS SECTION ===
        if viral_count > 0:
            st.markdown("### 🔥 Trending Viral Posts")
            viral_posts = df[df["is_viral"] == True].nlargest(5, "likes")
            
            for idx, post in viral_posts.iterrows():
                verified = "✓" if post["is_verified"] else ""
                col_a, col_b = st.columns([3, 1])
                with col_a:
                    st.markdown(f"""
                    <div class="viral-post">
                        <strong>🔥 @{post['username']}</strong> {verified} on <strong>{post['platform']}</strong><br>
                        <small>{post['topic']} • {post['post_type']} • {post['content_type']}</small><br>
                        👍 {post['likes']:,} | 💬 {post['comments']:,} | 🔄 {post['shares']:,} | 📈 {post['engagement_rate']:.1f}%
                    </div>
                    """, unsafe_allow_html=True)
                with col_b:
                    st.markdown(f"<small>{post['timestamp'].strftime('%H:%M:%S')}</small>", unsafe_allow_html=True)

        # === ANALYTICS VISUALIZATIONS ===
        st.markdown("### 📊 Analytics & Insights")
        
        # Row 1: Engagement and Platform Distribution
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            # Engagement over time
            if len(df) > 10:
                time_df = df.sort_values("timestamp").tail(100)
                fig_time = px.line(time_df, x="timestamp", y="likes", 
                                  color="platform",
                                  title="👍 Likes Over Time (Last 100 Posts)",
                                  labels={"likes": "Likes", "timestamp": "Time"})
                fig_time.update_layout(height=400)
                st.plotly_chart(fig_time, use_container_width=True, key="likes_time")
        
        with chart_col2:
            # Platform distribution
            platform_dist = df["platform"].value_counts().reset_index()
            platform_dist.columns = ["platform", "count"]
            fig_platform = px.pie(platform_dist, values="count", names="platform",
                                 title="📱 Posts by Platform",
                                 color_discrete_sequence=px.colors.qualitative.Set3)
            fig_platform.update_layout(height=400)
            st.plotly_chart(fig_platform, use_container_width=True, key="platform_pie")

        # Row 2: Sentiment and Topic Analysis
        chart_col3, chart_col4 = st.columns(2)
        
        with chart_col3:
            # Sentiment analysis
            sentiment_data = df.groupby("sentiment").agg({
                "likes": "sum",
                "comments": "sum",
                "shares": "sum"
            }).reset_index()
            
            fig_sentiment = go.Figure()
            fig_sentiment.add_trace(go.Bar(name="Likes", x=sentiment_data["sentiment"], y=sentiment_data["likes"]))
            fig_sentiment.add_trace(go.Bar(name="Comments", x=sentiment_data["sentiment"], y=sentiment_data["comments"]))
            fig_sentiment.add_trace(go.Bar(name="Shares", x=sentiment_data["sentiment"], y=sentiment_data["shares"]))
            fig_sentiment.update_layout(
                title="😊 Engagement by Sentiment",
                barmode="group",
                height=400,
                xaxis_title="Sentiment",
                yaxis_title="Count"
            )
            st.plotly_chart(fig_sentiment, use_container_width=True, key="sentiment_bar")
        
        with chart_col4:
            # Top topics
            topic_engagement = df.groupby("topic").agg({
                "likes": "sum",
                "post_id": "count"
            }).reset_index()
            topic_engagement.columns = ["topic", "total_likes", "post_count"]
            topic_engagement = topic_engagement.sort_values("total_likes", ascending=False).head(8)
            
            fig_topic = px.bar(topic_engagement, x="topic", y="total_likes",
                              title="📌 Top Topics by Engagement",
                              labels={"total_likes": "Total Likes", "topic": "Topic"},
                              color="total_likes",
                              color_continuous_scale="Viridis")
            fig_topic.update_layout(height=400)
            st.plotly_chart(fig_topic, use_container_width=True, key="topic_bar")

        # Row 3: Content Type and Geographic Distribution
        chart_col5, chart_col6 = st.columns(2)
        
        with chart_col5:
            # Content type performance
            content_perf = df.groupby("content_type").agg({
                "likes": "mean",
                "comments": "mean",
                "shares": "mean",
                "engagement_rate": "mean"
            }).reset_index()
            
            fig_content = px.bar(content_perf, x="content_type", y="engagement_rate",
                                title="📹 Avg Engagement Rate by Content Type",
                                labels={"engagement_rate": "Avg Engagement Rate (%)", "content_type": "Content Type"},
                                color="engagement_rate",
                                color_continuous_scale="Blues")
            fig_content.update_layout(height=400)
            st.plotly_chart(fig_content, use_container_width=True, key="content_bar")
        
        with chart_col6:
            # Geographic distribution
            location_df = df[df["location"].notna()]
            if not location_df.empty:
                geo_data = location_df.groupby("location").agg({
                    "post_id": "count",
                    "likes": "sum"
                }).reset_index()
                geo_data.columns = ["location", "posts", "total_likes"]
                geo_data = geo_data.sort_values("posts", ascending=False).head(10)
                
                fig_geo = px.bar(geo_data, x="location", y="posts",
                               title="🌍 Top Locations by Post Volume",
                               labels={"posts": "Number of Posts", "location": "Location"},
                               color="total_likes",
                               color_continuous_scale="Reds")
                fig_geo.update_layout(height=400)
                st.plotly_chart(fig_geo, use_container_width=True, key="geo_bar")
            else:
                st.info("No location data available")

        # Row 4: Hashtag Analysis and Post Type Distribution
        chart_col7, chart_col8 = st.columns(2)
        
        with chart_col7:
            # Top hashtags
            all_hashtags = []
            for hashtags in df["hashtags"]:
                if hashtags:
                    all_hashtags.extend(hashtags)
            
            if all_hashtags:
                from collections import Counter
                hashtag_counts = Counter(all_hashtags).most_common(15)
                hashtag_df = pd.DataFrame(hashtag_counts, columns=["hashtag", "count"])
                
                fig_hashtags = px.bar(hashtag_df, x="count", y="hashtag",
                                     orientation="h",
                                     title="#️⃣ Top 15 Trending Hashtags",
                                     labels={"count": "Occurrences", "hashtag": "Hashtag"},
                                     color="count",
                                     color_continuous_scale="Greens")
                fig_hashtags.update_layout(height=400)
                st.plotly_chart(fig_hashtags, use_container_width=True, key="hashtag_bar")
            else:
                st.info("No hashtag data available")
        
        with chart_col8:
            # Post type by platform
            post_type_data = df.groupby(["platform", "post_type"]).size().reset_index(name="count")
            
            fig_post_type = px.sunburst(post_type_data, 
                                       path=["platform", "post_type"], 
                                       values="count",
                                       title="📄 Post Types by Platform")
            fig_post_type.update_layout(height=400)
            st.plotly_chart(fig_post_type, use_container_width=True, key= "post_type_sunburst")

        # === INFLUENCER LEADERBOARD ===
        st.markdown("### ⭐ Top Influencers")
        influencer_data = df[df["is_influencer"] == True].groupby(["username", "platform"]).agg({
            "post_id": "count",
            "likes": "sum",
            "followers": "max",
            "engagement_rate": "mean"
        }).reset_index()
        influencer_data.columns = ["Username", "Platform", "Posts", "Total Likes", "Followers", "Avg Engagement %"]
        influencer_data = influencer_data.sort_values("Total Likes", ascending=False).head(10)
        
        if not influencer_data.empty:
            st.dataframe(influencer_data, use_container_width=True, height=300)
        else:
            st.info("No influencer data available in current filters")

        # === RAW DATA TABLE ===
        st.markdown("### 📋 Recent Posts")
        display_df = df.head(20)[[
            "post_id", "username", "platform", "topic", "post_type", 
            "sentiment", "likes", "comments", "shares", "engagement_rate", 
            "is_viral", "is_verified", "timestamp"
        ]]
        
        # Style the dataframe
        def highlight_viral(row):
            return ['background-color: #fff3cd' if row['is_viral'] else '' for _ in row]
        
        styled_df = display_df.style.apply(highlight_viral, axis=1)
        st.dataframe(styled_df, use_container_width=True, height=500)

        # === FOOTER ===
        st.markdown("---")
        col_footer1, col_footer2, col_footer3, col_footer4 = st.columns(4)
        
        with col_footer1:
            st.caption(f"🕐 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        with col_footer2:
            st.caption(f"🔄 Auto-refresh: {update_interval}s")
        with col_footer3:
            st.caption(f"📊 Displaying {total_posts:,} posts")
        with col_footer4:
            positive_pct = (len(df[df['sentiment'] == 'positive']) / total_posts * 100) if total_posts > 0 else 0
            st.caption(f"😊 {positive_pct:.1f}% Positive sentiment")

    time.sleep(update_interval)