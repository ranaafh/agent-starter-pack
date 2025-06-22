#!/usr/bin/env python3
"""
Test script to ask specific question about BI engineers onboarding process
"""

import json
import vertexai.agent_engines

def test_onboarding_question():
    """Test the deployed Agent Engine with onboarding question."""
    
    # Load the deployed agent ID from metadata
    try:
        with open("deployment_metadata.json", "r") as f:
            metadata = json.load(f)
        
        agent_engine_id = metadata["remote_agent_engine_id"]
        print(f"🤖 Testing Remote Agent Engine with Onboarding Question:")
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

    # Ask the specific question about BI engineers onboarding
    question = "Can you search for and tell me about the onboarding process for BI engineers at Badal?"
    
    print(f"📤 Asking: '{question}'")
    print("📥 Response:")
    print("-" * 80)
    
    try:
        # Stream the response
        full_response = ""
        for event in remote_agent_engine.stream_query(message=question, user_id="test_user"):
            if isinstance(event, dict):
                # Print the event for debugging
                print(f"Event: {event}")
                
                # Extract text content if available
                if 'content' in event and 'parts' in event['content']:
                    for part in event['content']['parts']:
                        if 'text' in part:
                            text = part['text']
                            full_response += text
                            print(text, end='', flush=True)
            
        print()
        print("-" * 80)
        print(f"📋 Complete Response:")
        print(full_response)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

    print()
    print("✅ Test completed!")

if __name__ == "__main__":
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║          🧪 TESTING BI ONBOARDING QUESTION 🧪             ║
    ╚═══════════════════════════════════════════════════════════╝
    """)
    
    test_onboarding_question() 