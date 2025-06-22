#!/usr/bin/env python3
"""
Test script for remote Agent Engine based on adk_app_testing.ipynb
This script tests the deployed Agent Engine remotely.
"""

import json
import vertexai.agent_engines

def test_remote_agent_engine():
    """Test the deployed Agent Engine remotely."""
    
    # Load the deployed agent ID from metadata
    try:
        with open("deployment_metadata.json", "r") as f:
            metadata = json.load(f)
        
        agent_engine_id = metadata["remote_agent_engine_id"]
        print(f"🤖 Testing Remote Agent Engine:")
        print(f"   ID: {agent_engine_id}")
        print(f"   Deployed: {metadata.get('deployment_timestamp', 'Unknown')}")
        print()
        
    except FileNotFoundError:
        print("❌ deployment_metadata.json not found!")
        print("   Make sure you've deployed the agent with 'make backend' first.")
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

    # Test with a simple query
    test_messages = [
        "Hello! Can you introduce yourself?",
        "What can you help me with?",
        "Thank you!"
    ]
    
    for i, message in enumerate(test_messages, 1):
        print(f"📤 Test {i}/3: Sending message: '{message}'")
        print("📥 Response:")
        
        try:
            # Stream the response (like in the notebook)
            response_text = ""
            for event in remote_agent_engine.stream_query(message=message, user_id="test_user"):
                print(f"   Event: {event}")
                # Try to extract text from the event if it's a dict
                if isinstance(event, dict) and 'text' in event:
                    response_text += event['text']
                elif hasattr(event, 'text'):
                    response_text += event.text
            
            if response_text:
                print(f"   📋 Full Response: {response_text}")
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
        
        print("-" * 60)
        print()

    # Test feedback registration
    print("📝 Testing feedback registration...")
    try:
        remote_agent_engine.register_feedback(
            feedback={
                "score": 5,
                "text": "Great response from automated test!",
                "invocation_id": "test-invocation-123", 
                "user_id": "test_user",
            }
        )
        print("✅ Feedback registered successfully!")
        
    except Exception as e:
        print(f"❌ Feedback registration failed: {e}")

    print()
    print("🎉 Remote Agent Engine testing completed!")

if __name__ == "__main__":
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║                                                           ║
    ║      🧪 TESTING REMOTE AGENT ENGINE 🧪                    ║
    ║                                                           ║
    ╚═══════════════════════════════════════════════════════════╝
    """)
    
    test_remote_agent_engine() 