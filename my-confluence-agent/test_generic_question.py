#!/usr/bin/env python3
"""
Test script to ask a generic question to see how the agent handles queries outside of company documentation
"""

import json
import vertexai.agent_engines

def test_generic_question():
    """Test the deployed Agent Engine with a generic question."""
    
    # Load the deployed agent ID from metadata
    try:
        with open("deployment_metadata.json", "r") as f:
            metadata = json.load(f)
        
        agent_engine_id = metadata["remote_agent_engine_id"]
        print(f"🤖 Testing Remote Agent Engine with Generic Question:")
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

    # Test with a generic question
    generic_questions = [
        "What is the capital of France?",
        "How do you make a good cup of coffee?",
        "What are the benefits of machine learning in healthcare?"
    ]
    
    for i, question in enumerate(generic_questions, 1):
        print(f"📤 Generic Question {i}/{len(generic_questions)}: '{question}'")
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

    print("✅ Generic question test completed!")

if __name__ == "__main__":
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║          🧪 TESTING GENERIC QUESTIONS 🧪                  ║
    ╚═══════════════════════════════════════════════════════════╝
    """)
    
    test_generic_question() 