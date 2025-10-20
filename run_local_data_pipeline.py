#!/usr/bin/env python3
"""
F1 Pipeline - Local Data Output Version
Saves extracted data to local-data folder for easy access and analysis.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def run_local_data_pipeline():
    """Run F1 pipeline with output to local-data folder."""
    print("🏎️ F1 Pipeline - Local Data Output")
    print("=" * 50)
    
    try:
        from f1_pipeline.config.settings import Settings, StorageConfig, DatabaseConfig
        from f1_pipeline.cloud_swap import CloudProviderFactory
        from f1_pipeline.extractors import DataExtractor
        import pandas as pd
        from datetime import datetime
        
        # Ensure local-data directory exists
        os.makedirs("./local-data", exist_ok=True)
        os.makedirs("./local-data/meetings", exist_ok=True)
        os.makedirs("./local-data/sessions", exist_ok=True) 
        os.makedirs("./local-data/drivers", exist_ok=True)
        os.makedirs("./local-data/laps", exist_ok=True)
        os.makedirs("./local-data/positions", exist_ok=True)
        
        # Setup with local-data path
        settings = Settings(
            environment="local",
            storage=StorageConfig(provider="local", local_path="./local-data"),
            database=DatabaseConfig(provider="local", db_path="./local-data/f1_data.db")
        )
        
        cloud_provider = CloudProviderFactory.create("local", settings.get_cloud_provider_config())
        extractor = DataExtractor(settings, cloud_provider)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        print("📊 Setting up local data extraction...")
        print(f"   Output folder: ./local-data/")
        print(f"   Timestamp: {timestamp}")
        print()
        
        # Step 1: Extract and save meetings
        print("📥 Step 1: Extracting 2023 F1 meetings...")
        meetings_df = extractor.openf1_client.get_meetings(year=2023)
        print(f"   Found: {len(meetings_df)} meetings")
        
        if len(meetings_df) > 0:
            meetings_file = f"./local-data/meetings/f1_meetings_2023_{timestamp}.csv"
            meetings_df.to_csv(meetings_file, index=False)
            print(f"   ✅ Saved to: {meetings_file}")
            
            # Also save as parquet for efficiency
            meetings_parquet = f"./local-data/meetings/f1_meetings_2023_{timestamp}.parquet"
            meetings_df.to_parquet(meetings_parquet, index=False)
            print(f"   ✅ Saved to: {meetings_parquet}")
        
        # Step 2: Extract and save sessions
        print(f"\n📥 Step 2: Extracting 2023 F1 sessions...")
        sessions_df = extractor.openf1_client.get_sessions(year=2023)
        print(f"   Found: {len(sessions_df)} sessions")
        
        if len(sessions_df) > 0:
            sessions_file = f"./local-data/sessions/f1_sessions_2023_{timestamp}.csv"
            sessions_df.to_csv(sessions_file, index=False)
            print(f"   ✅ Saved to: {sessions_file}")
            
            sessions_parquet = f"./local-data/sessions/f1_sessions_2023_{timestamp}.parquet"
            sessions_df.to_parquet(sessions_parquet, index=False)
            print(f"   ✅ Saved to: {sessions_parquet}")
        
        # Step 3: Extract detailed data for recent sessions
        print(f"\n📥 Step 3: Extracting detailed data for recent sessions...")
        recent_sessions = sessions_df.tail(3)  # Last 3 sessions for faster processing
        
        all_drivers = []
        all_laps = []
        all_positions = []
        
        for idx, session in recent_sessions.iterrows():
            session_key = session['session_key']
            session_name = session.get('session_name', 'Unknown')
            meeting_name = session.get('meeting_key', 'Unknown')
            
            print(f"   Processing: {session_name} (session_key: {session_key})")
            
            try:
                # Get drivers
                drivers_df = extractor.openf1_client.get_drivers(session_key=session_key)
                if len(drivers_df) > 0:
                    drivers_df['session_name'] = session_name
                    drivers_df['meeting_key'] = meeting_name
                    all_drivers.append(drivers_df)
                    print(f"     Drivers: {len(drivers_df)} records")
                
                # Get laps (for race sessions)
                if 'race' in session_name.lower() or 'qualifying' in session_name.lower():
                    laps_df = extractor.openf1_client.get_laps(session_key=session_key)
                    if len(laps_df) > 0:
                        laps_df['session_name'] = session_name
                        laps_df['meeting_key'] = meeting_name
                        all_laps.append(laps_df)
                        print(f"     Laps: {len(laps_df)} records")
                
                # Get positions (sample of position data)
                position_df = extractor.openf1_client.get_position(session_key=session_key)
                if len(position_df) > 0:
                    # Take a sample to avoid huge files
                    position_sample = position_df.sample(n=min(100, len(position_df)))
                    position_sample['session_name'] = session_name
                    position_sample['meeting_key'] = meeting_name
                    all_positions.append(position_sample)
                    print(f"     Positions: {len(position_sample)} records (sampled)")
                    
            except Exception as e:
                print(f"     ⚠️ Error: {str(e)[:60]}...")
                continue
        
        # Step 4: Save detailed data to files
        print(f"\n💾 Step 4: Saving detailed data to local files...")
        
        # Save drivers data
        if all_drivers:
            combined_drivers = pd.concat(all_drivers, ignore_index=True)
            drivers_file = f"./local-data/drivers/f1_drivers_recent_{timestamp}.csv"
            combined_drivers.to_csv(drivers_file, index=False)
            print(f"   ✅ Drivers saved: {drivers_file} ({len(combined_drivers)} records)")
            
        # Save laps data  
        if all_laps:
            combined_laps = pd.concat(all_laps, ignore_index=True)
            laps_file = f"./local-data/laps/f1_laps_recent_{timestamp}.csv"
            combined_laps.to_csv(laps_file, index=False)
            print(f"   ✅ Laps saved: {laps_file} ({len(combined_laps)} records)")
            
        # Save positions data
        if all_positions:
            combined_positions = pd.concat(all_positions, ignore_index=True)
            positions_file = f"./local-data/positions/f1_positions_recent_{timestamp}.csv"
            combined_positions.to_csv(positions_file, index=False)
            print(f"   ✅ Positions saved: {positions_file} ({len(combined_positions)} records)")
        
        # Step 5: Create summary report
        print(f"\n📋 Step 5: Creating summary report...")
        summary_data = {
            "extraction_timestamp": timestamp,
            "year": 2023,
            "meetings_count": len(meetings_df),
            "sessions_count": len(sessions_df),
            "processed_sessions": len(recent_sessions),
            "drivers_records": len(combined_drivers) if all_drivers else 0,
            "laps_records": len(combined_laps) if all_laps else 0,
            "positions_records": len(combined_positions) if all_positions else 0
        }
        
        summary_df = pd.DataFrame([summary_data])
        summary_file = f"./local-data/extraction_summary_{timestamp}.csv"
        summary_df.to_csv(summary_file, index=False)
        print(f"   ✅ Summary saved: {summary_file}")
        
        # Final summary
        print(f"\n🎉 F1 Data Extraction Complete!")
        print(f"=" * 50)
        print(f"📁 All data saved to: ./local-data/")
        print(f"📊 Summary:")
        print(f"   • {summary_data['meetings_count']} meetings")
        print(f"   • {summary_data['sessions_count']} sessions")  
        print(f"   • {summary_data['drivers_records']} driver records")
        print(f"   • {summary_data['laps_records']} lap records")
        print(f"   • {summary_data['positions_records']} position records")
        print()
        print(f"📂 File structure:")
        print(f"   ./local-data/meetings/     - F1 meeting information")
        print(f"   ./local-data/sessions/     - Session details")
        print(f"   ./local-data/drivers/      - Driver information")
        print(f"   ./local-data/laps/         - Lap times and data")
        print(f"   ./local-data/positions/    - Position tracking")
        print(f"   ./local-data/*.csv         - Summary reports")
        print()
        print(f"💡 Next steps:")
        print(f"   • Open CSV files in Excel or your favorite data tool")
        print(f"   • Use Python/pandas to analyze the data")
        print(f"   • Modify the script to extract more sessions or data types")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_local_data_pipeline()
    if success:
        print(f"\n🏁 Ready to analyze your F1 data!")
    else:
        print(f"\n❌ Check the error messages above for troubleshooting.")