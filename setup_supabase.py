import subprocess
import sys
import os

def install_requirements():
    """Install required packages"""
    print("🚀 Installing required packages...")
    
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements_supabase.txt"])
        print("✅ All packages installed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install packages: {e}")
        return False

def check_env_file():
    """Check if environment file exists and guide user"""
    print("\n🔍 Checking environment configuration...")
    
    if os.path.exists("supabase_config.env"):
        print("✅ supabase_config.env file found")
        
        # Check if user has updated the values
        with open("supabase_config.env", "r") as f:
            content = f.read()
            
        if "your_supabase_url_here" in content:
            print("\n⚠️  Please update supabase_config.env with your actual Supabase credentials:")
            print("   1. SUPABASE_URL - Your Supabase project URL")
            print("   2. SUPABASE_KEY - Your Supabase anon key")
            print("   3. SUPABASE_SERVICE_ROLE_KEY - Your Supabase service role key")
            print("\n   You can find these in your Supabase project settings.")
            return False
        else:
            print("✅ Environment file appears to be configured")
            return True
    else:
        print("❌ supabase_config.env file not found")
        return False

def check_excel_file():
    """Check if required Excel file exists"""
    print("\n🔍 Checking required files...")
    
    if os.path.exists("Unique_States_Districts.xlsx"):
        print("✅ Unique_States_Districts.xlsx found")
        return True
    else:
        print("❌ Unique_States_Districts.xlsx not found")
        print("   Please make sure this file exists in the current directory")
        return False

def main():
    """Main setup function"""
    print("🌾 Maharashtra Groundwater Supabase Setup")
    print("=" * 50)
    
    # Install requirements
    if not install_requirements():
        return False
    
    # Check environment file
    env_ok = check_env_file()
    
    # Check Excel file
    excel_ok = check_excel_file()
    
    if env_ok and excel_ok:
        print("\n🎉 Setup completed successfully!")
        print("✅ You can now run: python maharashtra_groundwater_supabase.py")
        return True
    else:
        print("\n⚠️  Setup incomplete. Please fix the issues above before running the main script.")
        return False

if __name__ == "__main__":
    main()
