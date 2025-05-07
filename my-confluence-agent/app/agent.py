# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# mypy: disable-error-code="arg-type"
import os

import google
import vertexai
from google.adk.agents import Agent
from langchain_google_vertexai import VertexAIEmbeddings

from app.retrievers import get_compressor, get_retriever
from app.templates import format_docs

EMBEDDING_MODEL = "text-embedding-005"
LOCATION = "us-central1"
LLM = "gemini-2.0-flash-001"

credentials, project_id = google.auth.default()
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", project_id)
os.environ.setdefault("GOOGLE_CLOUD_LOCATION", LOCATION)
os.environ.setdefault("GOOGLE_GENAI_USE_VERTEXAI", "True")

vertexai.init(project=project_id, location=LOCATION)
embedding = VertexAIEmbeddings(
    project=project_id, location=LOCATION, model_name=EMBEDDING_MODEL
)


EMBEDDING_COLUMN = "embedding"
TOP_K = 5

data_store_region = os.getenv("DATA_STORE_REGION", "us")
data_store_id = os.getenv("DATA_STORE_ID", "my-agentic-rag-datastore")

retriever = get_retriever(
    project_id=project_id,
    data_store_id=data_store_id,
    data_store_region=data_store_region,
    embedding=embedding,
    embedding_column=EMBEDDING_COLUMN,
    max_documents=10,
)

compressor = get_compressor(
    project_id=project_id,
)


def retrieve_docs(query: str) -> str:
    """
    Useful for retrieving relevant documents based on a query.
    Use this when you need additional information to answer a question.

    Args:
        query (str): The user's question or search query.

    Returns:
        str: Formatted string containing relevant document content.
             Returns a specific string or marker if no relevant docs are found or an error occurs.
    """
    retrieved_docs = [] # Initialize to an empty list
    try:
        # Use the retriever to fetch relevant documents based on the query
        print(f"Attempting to retrieve documents for query: '{query}'") # Log the start of retrieval
        retrieved_docs = retriever.invoke(query)
        print(f"Retrieved {len(retrieved_docs)} documents for query: '{query}'") # Log number of documents retrieved

        # Check if retrieval returned any documents
        if not retrieved_docs:
            print(f"No documents retrieved for query: '{query}'") # Log this event
            return "NO_RELEVANT_DOCUMENTS_FOUND" # Return a specific marker

        # Re-rank docs with Vertex AI Rank for better relevance
        # This call will only happen if retrieved_docs is NOT empty
        ranked_docs = compressor.compress_documents(
            documents=retrieved_docs, query=query
        )
        print(f"Ranked down to {len(ranked_docs)} documents after compression for query: '{query}'") # Log number of documents after ranking

        # Optional: Check if ranking returned any documents (less common, but possible)
        if not ranked_docs:
             print(f"No documents remained after ranking for query: '{query}'") # Log this event
             return "NO_RELEVANT_DOCUMENTS_FOUND" # Return the same marker


        # Format ranked documents into a consistent structure for LLM consumption
        formatted_docs = format_docs.format(docs=ranked_docs)
        # print(f"Formatted documents: {formatted_docs}") # Optional: Log the formatted docs (can be verbose)

    except Exception as e:
        # This block now catches other errors (network, API issues other than empty records)
        print(f"Error calling retrieval tool with query: '{query}'. Error: {type(e).__name__}: {e}") # Log the error internally
        # Return a different marker or handle this error type separately if needed
        return "RETRIEVAL_TOOL_ERROR" # Signal failure to the agent

    print(f"Successfully retrieved, ranked, and formatted documents for query: '{query}'") # Log successful completion
    return formatted_docs


instruction = """You are an AI assistant for question-answering tasks.
Answer to the best of your ability using the context provided.
Leverage the Tools you are provided to answer questions.
If you already know the answer to a question, and if asked outside of the context provided,you can respond directly without using the tools.
In case of an error, inform the user that you could not retieve the information. 
"""

root_agent = Agent(
    name="root_agent",
    model="gemini-2.0-flash",
    instruction=instruction,
    tools=[retrieve_docs],
)
