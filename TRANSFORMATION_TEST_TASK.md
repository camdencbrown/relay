# Fresh Agent Task - Transformation Test

## Your Mission

You have access to a data movement API at: `http://localhost:8001`

**Task:** Answer this business question:

> "Show me each user's name along with how many posts they've written. Order by most posts first."

## What You Know

1. There is a data API at http://localhost:8001
2. The API has multiple datasets available
3. You'll need to combine data from different sources
4. You should discover what's available and figure out how to join them

## Constraints

- **Do NOT ask for help** - figure it out using the API
- **Document your discovery process**
- **Explain how you combined the data**
- **Show the final answer**

## Expected Output Format

```
User Name         | Post Count
------------------|------------
Alice Johnson     | 10
Bob Smith         | 10
...
```

## Success Criteria

- ✅ Discover there are separate users and posts datasets
- ✅ Figure out how they relate (join key)
- ✅ Combine them appropriately
- ✅ Generate the answer with user NAMES (not just IDs)
- ✅ Order by post count descending

## Hints (Try Without First!)

<details>
<summary>Only open if stuck after 10 minutes</summary>

- Look for a way to search for datasets
- There should be users data and posts data
- Think about what field connects them
- The API might have transformation capabilities

</details>

---

**Begin when ready!**
