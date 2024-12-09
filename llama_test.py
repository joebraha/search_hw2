import json
import time
from llamaapi import LlamaAPI

def get_summary(llama, prompt):
    
    api_request_json = {
        "model": "llama3.2-3b",
        "messages": [
            {"role": "user", "content": prompt},
        ],
    }

    # Execute the Request
    response = llama.run(api_request_json)
    response_json = response.json()

    # Extract and print the message content
    choices = response_json.get('choices', [])
    for choice in choices:
        message = choice.get('message', {})
        content = message.get('content', '')
        # print(content)

    return content


# take in 10 documents from 10_documents.txt
with open('10_documents.txt', 'r') as file:
    documents = file.readlines()

# create modified document file
with open('10_documents_modified.txt', 'w') as file:

    # Initialize the SDK
    llama = LlamaAPI("LA-937020629bf94579b02d38621d0d379f4acdc92d6a7e4724a2168c1645318248")

    prompt_prefix = "Rephrase this passage of text in simpler language and only output the generated content, no notes or introductions needed, no line breaks: "
    start_time = time.time()  # Start the timer

    for document in documents:
        document = document.strip()
        test_prompt = prompt_prefix + document
        summary = get_summary(llama, test_prompt)
        modified_document = document + " " + summary + "\n"
        file.write(modified_document)
    
    end_time = time.time()  # End the timer
    elapsed_time = end_time - start_time
    print(f"Modified documents have been written to 10_documents_modified.txt in {elapsed_time:.2f} seconds")
