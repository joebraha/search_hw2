from ollama import chat, ChatResponse, Client

modelname = "reword"


client = Client(
    host="http://localhost:11434",
    # headers={'x-some-header': 'some-value'}
)
response = client.chat(
    model=modelname,
    messages=[
        {
            "role": "user",
            "content": "Why is the sky blue?",
        },
    ],
)

with open("10_documents.txt") as f:
    for doc in f:
        print(doc)
        response: ChatResponse = client.chat(
            model=modelname,
            messages=[
                {
                    "role": "user",
                    "content": doc,
                },
            ],
        )
        print(response.message.content)
