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

# doc expansion
# msg_content = "Rephrase this passage of text using as many new words as is possible and only output the generated content, no notes or introductions needed, no line breaks, ignore the number at the beginning: "

# query expansion
msg_content = "Come up with 5 queries that a user might submit to a search engine looking for this passage- use as much new phrasing as possible. Only return the queries, ignore the number at the beginning, include no extra text as introduction. Passage: "

# take in 10 documents from 10_documents.txt
with open('random_filtered_docs.txt', 'r') as file:
    documents = file.readlines()

with open('10k_docs_query_expansion.txt', 'w') as file:
    start_time = time.time()  # Start the timer
    doc_counter = 0
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
        doc_queries_stripped += "\n"
        # modified_document = stripped_doc + " " + doc_queries_stripped + "\n"
        file.write(doc_queries_stripped)
        doc_counter += 1
        if (doc_counter % 100 == 0):
            print(f"Processed {doc_counter} documents in {time.time() - start_time:.2f} seconds")

    end_time = time.time()  # End the timer
    elapsed_time = end_time - start_time
    print(f"results have been written to file in {elapsed_time:.2f} seconds")
