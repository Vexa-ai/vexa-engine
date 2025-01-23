# Chat API Usage Guide

## Authentication

### 1. Google Authentication

To authenticate with Google:

```bash
POST /auth/google
Content-Type: application/json

{
    "token": "YOUR_GOOGLE_TOKEN",
    "utm_source": "optional_source",      # Optional: UTM tracking
    "utm_medium": "optional_medium",      # Optional: UTM tracking
    "utm_campaign": "optional_campaign",  # Optional: UTM tracking
    "utm_term": "optional_term",          # Optional: UTM tracking
    "utm_content": "optional_content",    # Optional: UTM tracking
    "ref": "optional_referrer"            # Optional: Referrer information
}
```

Response:
```json
{
    "user_id": "user_uuid",
    "user_name": "User Name",
    "image": "profile_image_url"
}
```

### 2. Token Authentication

To authenticate with an existing token:

```bash
POST /submit_token
Content-Type: application/json

{
    "token": "YOUR_INTERNAL_TOKEN"
}
```

Response:
```json
{
    "user_id": "user_uuid",
    "user_name": "User Name",
    "image": "profile_image_url"
}
```

### Using the Authentication Token

After authentication, use the token in all subsequent requests:
```bash
Authorization: Bearer YOUR_TOKEN
```

## Starting a New Chat

To start a new chat conversation, make a POST request to `/chat` endpoint:

```bash
POST /chat
Content-Type: application/json
Authorization: Bearer YOUR_TOKEN

{
    "query": "What was discussed about marketing?",
    "entities": ["John Smith", "Jane Doe"],  # Optional: Filter by speakers
    "model": "gpt-4o-mini",                 # Optional: Default is gpt-4o-mini
    "temperature": 0.7,                      # Optional: Control response randomness
    "meeting_ids": ["uuid1", "uuid2"]       # Optional: Specific meetings context
}
```

### Response Format
The response is a Server-Sent Events (SSE) stream with two types of events:

1. Streaming chunks during generation:
```json
{
    "chunk": "partial response text..."
}
```

2. Final response with metadata:
```json
{
    "thread_id": "thread_123",
    "linked_output": "Response with [1](/meeting/uuid1) references",
    "service_content": {
        "output": "Raw response text",
        "context": "Context used for generation"
    }
}
```

## Continuing a Conversation

To continue the conversation, include the `thread_id` from the previous response:

```bash
POST /chat
Content-Type: application/json
Authorization: Bearer YOUR_TOKEN

{
    "query": "Tell me more about that marketing strategy",
    "thread_id": "thread_123"  # From previous response
}
```

## Working with Meeting References

The API provides two formats for responses:

1. `linked_output`: Contains markdown-formatted links to meetings
   - Example: "The marketing plan [1](/meeting/uuid1) was discussed..."

2. `service_content.output`: Raw text without formatting
   - Example: "The marketing plan [1] was discussed..."

### Best Practices

1. For displaying responses:
   - Use `linked_output` for clickable meeting references, substitute rendered assistant message with this

2. Thread Management:
   - Always store the `thread_id` to maintain conversation context
   - Pass it in follow-up queries to maintain conversation history

3. Error Handling:
   - Check for error events in the SSE stream:
   ```json
   {
       "error": "Error message",
       "service_content": {"error": "Detailed error"}
   }
   ```

## Example Usage

```javascript
// Starting a new chat
const response = await fetch('/chat', {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer YOUR_TOKEN'
    },
    body: JSON.stringify({
        query: "What were the key decisions made?"
    })
});

const reader = response.body.getReader();
let thread_id;

// Process the SSE stream
while (true) {
    const {value, done} = await reader.read();
    if (done) break;
    
    const events = new TextDecoder().decode(value).split('\n\n');
    for (const event of events) {
        if (!event.startsWith('data: ')) continue;
        
        const data = JSON.parse(event.slice(6));
        
        if (data.chunk) {
            // Handle streaming chunk
            console.log(data.chunk);
        } else if (data.thread_id) {
            // Handle final response
            thread_id = data.thread_id;
            console.log('Thread ID:', thread_id);
            console.log('Linked Output:', data.linked_output);
        }
    }
}

// Continue conversation
const followUpResponse = await fetch('/chat', {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer YOUR_TOKEN'
    },
    body: JSON.stringify({
        query: "Tell me more about that",
        thread_id: thread_id
    })
});
```

## Complete Authentication and Chat Example

```javascript
// 1. Authenticate with Google
async function authenticateWithGoogle(googleToken) {
    const response = await fetch('/auth/google', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            token: googleToken,
            utm_source: 'web_app'
        })
    });
    
    const authData = await response.json();
    return authData;  // Contains user_id, user_name, and token
}

// 2. Or authenticate with internal token
async function authenticateWithToken(internalToken) {
    const response = await fetch('/submit_token', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            token: internalToken
        })
    });
    
    const authData = await response.json();
    return authData;  // Contains user_id, user_name, and token
}

// 3. Start chat after authentication
async function startChat(authToken, query) {
    const response = await fetch('/chat', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${authToken}`
        },
        body: JSON.stringify({ query })
    });

    // Handle streaming response
    const reader = response.body.getReader();
    let thread_id;
    let fullResponse = '';

    while (true) {
        const {value, done} = await reader.read();
        if (done) break;
        
        const events = new TextDecoder().decode(value).split('\n\n');
        for (const event of events) {
            if (!event.startsWith('data: ')) continue;
            
            const data = JSON.parse(event.slice(6));
            
            if (data.chunk) {
                // Accumulate streaming response
                fullResponse += data.chunk;
                // Update UI with streaming text
                console.log('Streaming:', data.chunk);
            } else if (data.thread_id) {
                // Final response - contains thread_id and linked content
                thread_id = data.thread_id;
                // Use linked_output for display (contains clickable meeting references)
                console.log('Final Response:', data.linked_output);
                // Store thread_id for follow-up messages
                localStorage.setItem('chat_thread_id', thread_id);
            }
        }
    }

    return { thread_id, fullResponse };
}

// Usage example
async function main() {
    try {
        // 1. Authenticate
        const googleToken = await getGoogleToken(); // Your Google token acquisition logic
        const authData = await authenticateWithGoogle(googleToken);
        
        // 2. Start chat
        const { thread_id, fullResponse } = await startChat(
            authData.token,
            "What were the key decisions in yesterday's marketing meeting?"
        );
        
        // 3. Continue conversation
        if (thread_id) {
            const followUp = await startChat(
                authData.token,
                "Tell me more about the budget discussion",
                thread_id
            );
        }
    } catch (error) {
        console.error('Error:', error);
    }
}

### Error Handling Best Practices

1. Authentication Errors:
```javascript
try {
    const authData = await authenticateWithGoogle(googleToken);
} catch (error) {
    if (error.status === 400) {
        console.error('Invalid token or token timing error');
    } else if (error.status === 401) {
        console.error('Authentication failed');
    } else {
        console.error('Server error:', error);
    }
}
```

2. Chat Errors:
```javascript
try {
    const chatResponse = await startChat(token, query);
} catch (error) {
    if (error.status === 401) {
        // Token expired or invalid
        console.error('Please re-authenticate');
    } else if (error.status === 403) {
        console.error('No access to requested meeting');
    } else {
        console.error('Chat error:', error);
    }
}
``` 