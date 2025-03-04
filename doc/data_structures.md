
### Extension expected responce for transcripts


#### request
/api/v1/transcription?meeting_id=pft-uyqe-gkj&token=918c9f3899d54f3bbcf445cd053c2304&last_msg_timestamp=2025-02-20T12:46:32.216Z

#### payload 

meeting_id=pft-uyqe-gkj&token=918c9f3899d54f3bbcf445cd053c2304&last_msg_timestamp=2025-02-20T12:46:32.216Z

! speaker_id, html_content_short, keywords to be removed

#### response
```json
[
    {
        "speaker": "Dmitriy Grankin",
        "speaker_id": "TBD", #to be removed
        "content": " .",
        "html_content": ".",
        "html_content_short": "", #to be removed
        "keywords": [], #to be removed
        "timestamp": "2025-02-20T12:50:46.694000Z"
    },
    {
        "speaker": "Dmitriy Grankin",
        "speaker_id": "TBD",
        "content": " so that the extension expects getting transcripts after the timestamp.",
        "html_content": "The extension expects to receive transcripts after the <b>timestamp</b>.",
        "html_content_short": "timestamp",
        "keywords": [],
        "timestamp": "2025-02-20T12:51:13.912000Z"
    },
    {
        "speaker": "Dmitriy Grankin",
        "speaker_id": "TBD",
        "content": " It provides last message timestamp.",
        "html_content": "It provides the <b>last message timestamp</b>.",
        "html_content_short": "last message timestamp",
        "keywords": [],
        "timestamp": "2025-02-20T12:51:20.360000Z"
    },
    {
        "speaker": "Dmitriy Grankin",
        "speaker_id": "TBD",
        "content": " Make sure it's compliant with that, but first you need to give your full explanation of the objectives and the task and ask questions for things that.",
        "html_content": "Make sure it's compliant with that. First, you need to provide a full explanation of the objectives and the tasks. Additionally, ask questions regarding any unclear aspects.",
        "html_content_short": "",
        "keywords": [],
        "timestamp": "2025-02-20T12:51:40.170000Z"
    },
    {
        "speaker": "Dmitriy Grankin",
        "speaker_id": "TBD",
        "content": " are unclear and ambiguous.",
        "html_content": "are unclear and ambiguous.",
        "html_content_short": "",
        "keywords": [],
        "timestamp": "2025-02-20T12:51:41.456000Z"
    },
    {
        "speaker": "Dmitriy Grankin",
        "speaker_id": "TBD",
        "content": " Let's open it.",
        "html_content": null,
        "html_content_short": null,
        "keywords": [],
        "timestamp": "2025-02-20T12:51:55.476000Z"
    },
    {
        "speaker": "Dmitriy Grankin",
        "speaker_id": "TBD",
        "content": " also catch how we're going how we're going to see the outdated script which is with",
        "html_content": null,
        "html_content_short": null,
        "keywords": [],
        "timestamp": "2025-02-20T12:55:30.296000Z"
    }
]