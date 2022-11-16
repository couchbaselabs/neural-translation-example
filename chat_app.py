import datetime
import os
import uuid
from datetime import timedelta
from typing import Optional

import streamlit as st
from couchbase.analytics import AnalyticsScanConsistency
from couchbase.auth import PasswordAuthenticator

from couchbase.exceptions import CouchbaseException
from dotenv import load_dotenv
from couchbase.options import (
    AnalyticsOptions,
    ClusterOptions,
    ClusterTimeoutOptions,
)
from couchbase.cluster import Cluster
from couchbase.exceptions import CouchbaseException
from dotenv import load_dotenv

# Read environment variables
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
BUCKET = os.getenv("BUCKET")


def get_db_connection() -> Cluster:
    """Create the connection to Couchbase Cluster"""

    # Authentication
    auth = PasswordAuthenticator(DB_USER, DB_PASSWORD)

    # Set query timeouts
    timeout_options = ClusterTimeoutOptions(
        kv_timeout=timedelta(seconds=5),
        query_timeout=timedelta(seconds=300),
        analytics_timeout=timedelta(seconds=600),
    )

    # Get a connection to our cluster
    cluster = Cluster.connect(
        f"couchbase://{DB_HOST}",
        ClusterOptions(auth, timeout_options=timeout_options),
    )

    return cluster


def write_message_to_db(cluster: Cluster, bucket: str, message: str) -> Optional[str]:
    """Store the messages from the application in the default collection of the specified bucket"""

    # Get a reference to our bucket
    cb = cluster.bucket(bucket)

    # Get a reference to the default collection
    cb_coll = cb.default_collection()

    # Store the message in a document in the collection
    try:
        doc_id = str(uuid.uuid4())
        message_doc = {
            "text": message,
            "ts": int(datetime.datetime.utcnow().timestamp()),
        }
        res = cb_coll.insert(doc_id, message_doc)
    except CouchbaseException as e:
        print(f"Error : {e}")
        return None
    else:
        return doc_id


def translate_message(cluster: Cluster, bucket: str, doc_id: str) -> tuple[str, int]:
    """Returns the translated message from the database for the specified message (document id)"""

    query = f'SELECT translate(m.text) as translation from `{bucket}`._default._default m where meta(m).id="{doc_id}"'

    # Query for the translation from the model in Couchbase Analytics
    # Note that the consistency is set to read the data from Data Service
    result = cluster.analytics_query(
        query, AnalyticsOptions(scan_consistency=AnalyticsScanConsistency.REQUEST_PLUS)
    )

    # Read results
    for row in result.rows():
        translated_message = row["translation"][-1]["translation_text"]

    # Get execution time reported by the server
    exec_time = result.metadata().metrics().execution_time()

    return translated_message, exec_time


def get_translated_messages(
    cluster: Cluster, bucket: str, limit: int = 10
) -> tuple[dict, int]:
    """Returns the last messages from the database upto the limit specified"""

    query = f"SELECT translate(m.text) as translation, m.ts, m.text as original_message from `{bucket}`._default._default m ORDER BY m.ts DESC limit {limit}"

    # Query for the translations from the model in Couchbase Analytics
    result = cluster.analytics_query(query)

    messages = []

    # Parse results
    for row in result.rows():
        message = {}
        try:
            message["ts"] = datetime.datetime.utcfromtimestamp(row["ts"])
            message["original_message"] = row["original_message"]
            message["translated_message"] = row["translation"][-1]["translation_text"]
            messages.append(message)
        except Exception as e:
            print(f"Exception while parsing results: {e}")
            continue

    # Get execution time reported by the server
    exec_time = result.metadata().metrics().execution_time()

    return messages, exec_time


# Set page layout
st.set_page_config(
    page_title="Let's Chat",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Title for app
st.title("Let's Chat")

# Get a database connection
cluster = get_db_connection()

# Message box
with st.form("chat", clear_on_submit=False):
    st.subheader("Message")
    message = st.text_input("Enter Message in your favorite language")

    submitted = st.form_submit_button("Send")

    # Translate the submitted message
    if submitted:

        # Store the message in database
        doc_id = write_message_to_db(cluster, BUCKET, message)

        if doc_id:
            with st.spinner("Translation in progress..."):

                # Get the translated message
                translated_message, exec_time = translate_message(
                    cluster, BUCKET, doc_id
                )

                st.subheader("Translated Message")
                st.text(translated_message)
                st.caption(f"Completed in {exec_time}s")
        else:
            st.text("Translation Failed")

# Show All Messages
show_chat = st.checkbox("Show all messages")

if show_chat:

    # Show all messages in the application
    with st.form("chat_feed", clear_on_submit=False):

        st.subheader("Chat Stream")
        messages_count = st.number_input("Messages to Fetch", min_value=1)
        fetch_messages = st.form_submit_button("Update message stream")

        if fetch_messages:

            # Fetch the last messages
            with st.spinner("Fetching Messages..."):
                tr_messages, exec_time = get_translated_messages(
                    cluster, BUCKET, messages_count
                )

                st.table(tr_messages)
                st.caption(f"Completed in {exec_time}s")
