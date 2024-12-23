from ollama import chat, ChatResponse, Client
import time

modelname = "llama3.2"


client = Client(
    host="http://localhost:11434",
    # headers={'x-some-header': 'some-value'}
)
# response = client.chat(
#     model=modelname,
#     messages=[
#         {
#             "role": "user",
#             "content": "Why is the sky blue?",
#         },
#     ],
# )

# msg_content = "Look at this passage of text and formulate 5 queries that could be given to a search engine about this passage. Format the response so it is one string of words, no numbering or line breaks. Passage: "
# msg_content = "Rephrase this passage of text using as many new words as is possible and only output the generated content, no notes or introductions needed, no line breaks: "
msg_content = "Come up with 5 queries that a user might submit to a search engine looking for this passage- use as much new phrasing as possible. Only return the queries. Passage: "

# take in 10 documents from 10_documents.txt
with open('10_documents.txt', 'r') as file:
    documents = file.readlines()

with open('10_documents_modified.txt', 'w') as file:
    start_time = time.time()  # Start the timer
    for doc in documents:
        stripped_doc = doc.strip()
        doc_msg = msg_content + stripped_doc
        response: ChatResponse = client.chat(
            model=modelname,
            messages=[
                {
                    "role": "user",
                    "content": doc_msg,
                },
            ],
        )
        doc_queries = response.message.content
        doc_queries_stripped = doc_queries.replace('\n', ' ')
        modified_document = stripped_doc + " " + doc_queries_stripped + "\n"
        file.write(modified_document)

    end_time = time.time()  # End the timer
    elapsed_time = end_time - start_time
    print(f"Modified documents have been written to 10_documents_modified.txt in {elapsed_time:.2f} seconds")
