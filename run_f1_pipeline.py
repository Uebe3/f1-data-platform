#!/usr/bin/env python3
"""
Improved F1 Pipeline with practical data extraction limits.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def run_practical_extraction():
    """Run F1 data extraction with practical limits."""
    print("🏎️ F1 Pipeline - Practical Data Extraction")
    print("=" * 50)
    
    try:
        from f1_pipeline.config.settings import Settings, StorageConfig, DatabaseConfig
        from f1_pipeline.cloud_swap import CloudProviderFactory
        from f1_pipeline.extractors import DataExtractor
        
        # Setup
        settings = Settings(
            environment="local",
            storage=StorageConfig(provider="local", local_path="./local-data"),
            database=DatabaseConfig(provider="local", db_path="./f1_local.db")
        )
        
        cloud_provider = CloudProviderFactory.create("local", settings.get_cloud_provider_config())
        extractor = DataExtractor(settings, cloud_provider)
        
        # Create tables
        print("📊 Setting up database...")
        extractor.create_raw_data_tables()
        
        # Extract basic data (fast)
        print("\n📥 Step 1: Extracting meetings and sessions...")
        meetings_df = extractor.openf1_client.get_meetings(year=2023)
        sessions_df = extractor.openf1_client.get_sessions(year=2023)
        
        print(f"   Meetings: {len(meetings_df)}")
        print(f"   Sessions: {len(sessions_df)}")
        
        # Save basic data
        if len(meetings_df) > 0:
            extractor._save_to_database(meetings_df, "raw_meetings")
        if len(sessions_df) > 0:
            extractor._save_to_database(sessions_df, "raw_sessions")
        
        # Extract data for just a few recent sessions (practical approach)
        print(f"\n📥 Step 2: Extracting detailed data for recent sessions...")
        recent_sessions = sessions_df.tail(5)  # Last 5 sessions
        
        total_records = 0
        for idx, session in recent_sessions.iterrows():
            session_key = session['session_key']
            session_name = session.get('session_name', 'Unknown')
            
            print(f"   Processing: {session_name} (key: {session_key})")
            
            # Extract key data types for this session
            try:
                drivers_df = extractor.openf1_client.get_drivers(session_key=session_key)
                if len(drivers_df) > 0:
                    extractor._save_to_database(drivers_df, "raw_drivers")
                    total_records += len(drivers_df)
                    print(f"     Drivers: {len(drivers_df)} records")
                
                # Try laps (if it's a race session)
                if 'race' in session_name.lower():
                    laps_df = extractor.openf1_client.get_laps(session_key=session_key)
                    if len(laps_df) > 0:
                        extractor._save_to_database(laps_df, "raw_laps") 
                        total_records += len(laps_df)
                        print(f"     Laps: {len(laps_df)} records")
                
            except Exception as e:
                print(f"     ⚠️ Error: {str(e)[:50]}...")
                continue
        
        print(f"\n✅ Extraction completed!")
        print(f"   Total detailed records: {total_records}")
        print(f"   Sessions processed: {len(recent_sessions)}")
        
        # Summary
        print(f"\n📊 Summary:")
        print(f"   ✅ {len(meetings_df)} meetings for 2023")
        print(f"   ✅ {len(sessions_df)} sessions for 2023") 
        print(f"   ✅ {total_records} detailed records from recent sessions")
        print(f"   💾 Data saved to: ./f1_local.db")
        
        print(f"\n💡 To extract more data:")
        print(f"   - Increase the number in recent_sessions.tail(5)")
        print(f"   - Add more endpoint types (positions, weather, etc.)")
        print(f"   - Run for different years")
        print(f"   - Use the full extract_year_data() for complete extraction")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_practical_extraction()
    if success:
        print(f"\n🏁 F1 Pipeline is working! Ready for Formula 1 data analysis!")
    else:
        print(f"\n❌ There were issues. Check the error messages above.")