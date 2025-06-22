#!/usr/bin/env python3
"""
Comprehensive test script with multiple questions to verify Agent Engine functionality
"""

import json
import vertexai.agent_engines

def test_multiple_questions():
    """Test the deployed Agent Engine with multiple specific questions."""
    
    # Load the deployed agent ID from metadata
    try:
        with open("deployment_metadata.json", "r") as f:
            metadata = json.load(f)
        
        agent_engine_id = metadata["remote_agent_engine_id"]
        print(f"🤖 Testing Remote Agent Engine with Multiple Questions:")
        print(f"   ID: {agent_engine_id}")
        print()
        
    except FileNotFoundError:
        print("❌ deployment_metadata.json not found!")
        return
    except KeyError:
        print("❌ remote_agent_engine_id not found in deployment_metadata.json")
        return

    # Connect to the remote agent engine
    try:
        print("🔌 Connecting to remote Agent Engine...")
        remote_agent_engine = vertexai.agent_engines.get(agent_engine_id)
        print("✅ Connected successfully!")
        print()
        
    except Exception as e:
        print(f"❌ Failed to connect to Agent Engine: {e}")
        return

    # List of questions to test
    questions = [
        "Tell me what data size is good for BigQuery + dbt + Looker Studio in terms of optimal tool combination",
        "What is badal sandbox for databricks?",
        "Any mention of eugene and zee in your docs?",
        "What does rana say about dbt exam?",
        "What link roshni provides for exam prep?",
        "What is stats canada project about?"
    ]
    
    for i, question in enumerate(questions, 1):
        print(f"📤 Question {i}/{len(questions)}: '{question}'")
        print("📥 Response:")
        print("-" * 80)
        
        try:
            # Stream the response
            full_response = ""
            for event in remote_agent_engine.stream_query(message=question, user_id="test_user"):
                if isinstance(event, dict):
                    # Extract text content if available
                    if 'content' in event and 'parts' in event['content']:
                        for part in event['content']['parts']:
                            if 'text' in part:
                                text = part['text']
                                full_response += text
                                print(text, end='', flush=True)
                
            print()
            print("-" * 80)
            print()
            
        except Exception as e:
            print(f"❌ Error: {e}")
            print("-" * 80)
            print()

    print("✅ Comprehensive test completed!")

if __name__ == "__main__":
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║        🧪 COMPREHENSIVE AGENT ENGINE TEST 🧪              ║
    ╚═══════════════════════════════════════════════════════════╝
    """)
    
    test_multiple_questions() 