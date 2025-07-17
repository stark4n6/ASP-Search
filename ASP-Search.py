import os
import urllib.request
import json
import sqlite3
import sys
from datetime import datetime
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import queue
import subprocess
from PIL import Image, ImageTk
import webbrowser

app_name = "ASP (App Store Package) Search"
version = "v1.1"

# Define the desired column order for the database table and text file output
DESIRED_COLUMN_ORDER = [
    "currentVersionReleaseDate",
    "releaseDate",
    "adamId",  # Primary key, representing the Apple ID / iTunes ID
    "trackName",
    "bundleId",  # This is the 'bundleId' from the API response (e.g., com.apple.Pages)
    "trackViewUrl",
    "artistName",
    "sellerName",
    "sellerUrl",
    "primaryGenreName",
    "error_message"  # Include error message if it's a possible column
]

# Define the keys we expect to parse directly from the API response.
# This list is derived from DESIRED_COLUMN_ORDER, excluding special handling fields.
PARSING_KEYS = [
    "currentVersionReleaseDate",
    "releaseDate",
    "trackName",
    "bundleId",
    "trackViewUrl",
    "artistName",
    "sellerName",
    "sellerUrl",
    "primaryGenreName"
]

# New constant for the metadata table
METADATA_TABLE_NAME = "metadata"

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)

def set_input_id_list(set_input_id_items):
    """
    Reads a list of IDs from a file or treats the input as a single ID.
    Deduplicates lines if the input is a file.
    """
    input_id_list = []
    if os.path.exists(set_input_id_items):
        try:
            with open(set_input_id_items, 'r') as f:
                # Use a set for deduplication
                unique_lines = set()
                for line in f:
                    line = line.strip()
                    if line:
                        unique_lines.add(line)
                input_id_list = list(unique_lines) # Convert back to list
        except Exception as e:
            return f"error: {e}", []
    # Check if the input is already a list (e.g., passed directly)
    elif isinstance(set_input_id_items, list):
        # Deduplicate the provided list
        input_id_list = list(set(item.strip() for item in set_input_id_items if item.strip()))
    else:
        input_id_list = [set_input_id_items]
    return None, input_id_list

def get_data_from_itunes(lookup_value, lookup_type):
    """
    Fetches application data from the iTunes API based on AdamID or BundleID.
    """
    response_json_data = None
    base_url = "http://itunes.apple.com/lookup?"
    
    if lookup_type == "adamId":
        url = f"{base_url}id={lookup_value}"
    elif lookup_type == "bundleId":
        url = f"{base_url}bundleId={lookup_value}"
    else:
        return f"ERROR: Invalid lookup type '{lookup_type}'. Must be 'adamId' or 'bundleId'.", None

    try:
        with urllib.request.urlopen(url) as response:
            response_data = response.read()
            response_json_data = json.loads(response_data)
    except Exception as e:
        return f"\nERROR fetching data for {lookup_value} ({lookup_type}): {e}", None
    return None, response_json_data

def parse_itunes_data(bundle_data, parsing_keys_list, original_lookup_value, lookup_type):
    """
    Parses the JSON response from the iTunes API into a flat dictionary.
    Handles cases where no data is found.
    """
    parsed_results_flat = {}
    
    if "resultCount" in bundle_data:
        if bundle_data["resultCount"] == 0:
            # No data found for the given ID
            if lookup_type == "adamId":
                parsed_results_flat["adamId"] = original_lookup_value
                parsed_results_flat["bundleId"] = "N/A"
            elif lookup_type == "bundleId":
                parsed_results_flat["adamId"] = "N/A"
                parsed_results_flat["bundleId"] = original_lookup_value

            parsed_results_flat["error_message"] = f"No data found at itunes.apple.com for: {original_lookup_value} (lookup by {lookup_type})"
        else:
            # Data found, process the first result
            data = (bundle_data["results"][0])
            
            # Set adamId based on lookup type or trackId
            if lookup_type == "adamId":
                parsed_results_flat["adamId"] = original_lookup_value
            elif lookup_type == "bundleId" and "trackId" in data:
                parsed_results_flat["adamId"] = str(data["trackId"])
            else:
                parsed_results_flat["adamId"] = "N/A"

            # Iterate through API response keys and extract desired ones
            for k, v in data.items():
                if k == "trackId" and lookup_type == "bundleId":
                    continue # Skip trackId if lookup was by bundleId, as we've already handled adamId
                if k == "bundleId": 
                    parsed_results_flat["bundleId"] = v
                    continue 

                if k in parsing_keys_list:
                    parsed_results_flat[k] = v
    return parsed_results_flat

def create_and_reorder_table(conn, cursor, table_name, desired_order, existing_columns):
    """
    Creates a new SQLite table with the desired column order or reorders an existing one.
    Data from the old table is migrated to the new one.
    """
    temp_table_name = f"{table_name}_temp"

    # Define column definitions for the new temporary table
    column_definitions = []
    for col in desired_order:
        if col == "adamId":
            column_definitions.append(f"{col} TEXT PRIMARY KEY")
        else:
            column_definitions.append(f"{col} TEXT")

    create_new_table_sql = f"CREATE TABLE IF NOT EXISTS {temp_table_name} ({', '.join(column_definitions)})"
    try:
        cursor.execute(create_new_table_sql)
        conn.commit()
    except sqlite3.Error as e:
        return f"Error creating temporary table: {e}", False

    # Get existing column names from the original table
    cursor.execute(f"PRAGMA table_info({table_name})")
    old_table_info = cursor.fetchall()
    old_column_names = [info[1] for info in old_table_info]

    # Map old column names to new ones if necessary (e.g., 'bundle_id_lookup' to 'adamId')
    column_mapping = {
        "bundle_id_lookup": "adamId",
    }

    columns_to_copy_select = []
    columns_to_copy_insert = []

    # Prepare lists of columns for INSERT and SELECT statements
    for new_col in desired_order:
        if new_col in old_column_names:
            columns_to_copy_select.append(new_col)
            columns_to_copy_insert.append(new_col)
        elif new_col in column_mapping and column_mapping[new_col] in old_column_names:
            columns_to_copy_select.append(column_mapping[new_col])
            columns_to_copy_insert.append(new_col)
        elif new_col == "adamId" and "trackId" in old_column_names and "bundle_id_lookup" not in old_column_names:
            columns_to_copy_select.append("trackId")
            columns_to_copy_insert.append("adamId")
        elif new_col == "bundleId" and "bundleId" in old_column_names:
            columns_to_copy_select.append("bundleId")
            columns_to_copy_insert.append("bundleId")

    # Remove duplicates and maintain order for insert/select columns
    temp_insert = []
    temp_select = []
    seen = set()
    for i, col in enumerate(columns_to_copy_insert):
        if col not in seen:
            seen.add(col)
            temp_insert.append(col)
            temp_select.append(columns_to_copy_select[i])

    columns_to_copy_insert = temp_insert
    columns_to_copy_select = temp_select

    # Check if the original table exists and copy data
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    if cursor.fetchone():
        if columns_to_copy_insert:
            insert_sql = f"INSERT INTO {temp_table_name} ({', '.join(columns_to_copy_insert)}) SELECT {', '.join(columns_to_copy_select)} FROM {table_name}"
            try:
                cursor.execute(insert_sql)
                conn.commit()
            except sqlite3.Error as e:
                conn.rollback()
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                conn.commit()
                return f"Error migrating data: {e}", False

    # Drop the old table
    drop_old_table_sql = f"DROP TABLE IF EXISTS {table_name}"
    try:
        cursor.execute(drop_old_table_sql)
        conn.commit()
    except sqlite3.Error as e:
        return f"Error dropping old table: {e}", False

    # Rename the temporary table to the original table name
    rename_table_sql = f"ALTER TABLE {temp_table_name} RENAME TO {table_name}"
    try:
        cursor.execute(rename_table_sql)
        conn.commit()
    except sqlite3.Error as e:
        return f"Error renaming table: {e}", False

    return None, True

class TextRedirector:
    """
    A custom stream handler to redirect stdout/stderr to a Tkinter Text widget
    via a queue, ensuring thread-safe updates.
    """
    def __init__(self, widget, q):
        self.widget = widget
        self.q = q

    def write(self, str_val):
        self.q.put(str_val)

    def flush(self):
        pass

class App(tk.Tk):
    """
    Main application class for the BundleID/AdamID Lookup GUI.
    """
    def __init__(self):
        super().__init__()
        self.title(f"{app_name} {version}")
        self.resizable(False, False) 
        self.geometry("750x650") 

        # --- Set application icon ---
        # Ensure 'app_icon.png' is in the same directory as your script
        # You can use different sizes for better display on various platforms
        icon_path = resource_path("assets/stark4n6.ico") # Replace with your icon file name
        if os.path.exists(icon_path):
            try:
                # Load the image using PIL
                icon_image = Image.open(icon_path)
                # Create a PhotoImage from the PIL image
                self.icon_photo = ImageTk.PhotoImage(icon_image)
                # Set the window icon
                self.iconphoto(True, self.icon_photo)
            except Exception as e:
                print(f"Warning: Could not load application icon from '{icon_path}': {e}")
        else:
            print(f"Warning: Application icon file '{icon_path}' not found.")
        # --- End icon setting ---

        self.actual_output_dir = None # Stores the actual directory where files will be saved
        self.logo_tk = None # To hold the PhotoImage object for the logo

        self.logo_image_path = resource_path("assets/asp.png") # Path to the logo image

        self.log_queue = queue.Queue() # Queue for thread-safe logging to the Text widget

        self.create_widgets()
        self.create_menu() # Call the new method to create the menu
        
        # Start processing the log queue for GUI updates
        self.process_queue() 
        # Redirect stdout and stderr to the custom TextRedirector
        sys.stdout = TextRedirector(self.output_text, self.log_queue)
        sys.stderr = TextRedirector(self.output_text, self.log_queue)

    def _format_path_for_display(self, path):
        """Converts a given path to use forward slashes for display."""
        if path:
            return path.replace(os.path.sep, '/')
        return path

    def process_queue(self):
        """
        Processes messages from the log queue and updates the Text widget.
        Called periodically by Tkinter's after method.
        """
        while not self.log_queue.empty():
            line = self.log_queue.get_nowait()
            self.output_text.insert(tk.END, line)
            self.output_text.see(tk.END) # Auto-scroll to the end
        self.after(100, self.process_queue) # Schedule itself to run again after 100ms

    def create_menu(self):
        """
        Creates the application's menu bar with File and Help options.
        """
        menu_bar = tk.Menu(self)
        self.config(menu=menu_bar) # Attach the menu bar to the window

        # --- File Menu ---
        file_menu = tk.Menu(menu_bar, tearoff=0)
        menu_bar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Exit", command=self.quit) # Exit the application

        # --- Help Menu ---
        help_menu = tk.Menu(menu_bar, tearoff=0)
        menu_bar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="GitHub", command=self.open_github_link)

    def open_github_link(self):
        """Opens the GitHub repository link in the default web browser."""
        try:
            webbrowser.open_new("https://github.com/stark4n6/asp-search")
        except Exception as e:
            self.log_queue.put(f"ERROR: Could not open GitHub link: {e}\n")
            messagebox.showerror("Error", f"Failed to open GitHub link. Please visit https://github.com/stark4n6/asp-search manually.\nError: {e}")


    def create_widgets(self):
        """
        Creates and arranges all the GUI widgets.
        """
        # Main container for left panel (input/output) and right panel (logo)
        main_container_frame = tk.Frame(self)
        main_container_frame.pack(fill="x", padx=10, pady=10) 

        # Left Panel: Input and Output Options
        left_panel_frame = ttk.Frame(main_container_frame)
        left_panel_frame.pack(side="left", anchor="nw", expand=False) 

        # Input Frame - Now inside left_panel_frame
        input_frame = ttk.LabelFrame(left_panel_frame, text="Input Details", padding="10")
        input_frame.pack(padx=5, pady=5, fill="x", anchor="nw") 
        input_frame.grid_columnconfigure(1, weight=1) # Allows the entry widget to expand

        ttk.Label(input_frame, text="AdamID/BundleID or File:").grid(row=0, column=0, sticky="w", pady=5)
        self.input_id_entry = ttk.Entry(input_frame, width=35) 
        self.input_id_entry.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        ttk.Button(input_frame, text="Browse File", command=self.browse_file).grid(row=0, column=2, padx=5, pady=5)

        ttk.Label(input_frame, text="Lookup Type:").grid(row=1, column=0, sticky="w", pady=5)
        self.lookup_type_var = tk.StringVar(value="adamId")
        ttk.Radiobutton(input_frame, text="AdamID", variable=self.lookup_type_var, value="adamId").grid(row=1, column=1, sticky="w")
        ttk.Radiobutton(input_frame, text="BundleID", variable=self.lookup_type_var, value="bundleId").grid(row=1, column=1, padx=80, sticky="w")

        # Output Options Frame - Now inside left_panel_frame
        output_options_frame = ttk.LabelFrame(left_panel_frame, text="Output Options", padding="10")
        output_options_frame.pack(padx=5, pady=5, fill="x", anchor="nw") 
        output_options_frame.grid_columnconfigure(1, weight=1) 

        ttk.Label(output_options_frame, text="Output Format:").grid(row=0, column=0, sticky="w", pady=5)
        self.output_format_var = tk.StringVar(value="console")
        # Trace changes to the output format variable to enable/disable folder Browse
        self.output_format_var.trace_add("write", self.on_output_format_change)

        ttk.Radiobutton(output_options_frame, text="Console", variable=self.output_format_var, value="console").grid(row=0, column=1, sticky="w", padx=5)
        ttk.Radiobutton(output_options_frame, text="Text File", variable=self.output_format_var, value="txt").grid(row=0, column=2, sticky="w", padx=5)
        ttk.Radiobutton(output_options_frame, text="SQLite DB", variable=self.output_format_var, value="db").grid(row=0, column=3, sticky="w", padx=5)
        ttk.Radiobutton(output_options_frame, text="Both (Text & DB)", variable=self.output_format_var, value="both").grid(row=0, column=4, sticky="w", padx=5)

        ttk.Label(output_options_frame, text="Output Folder:").grid(row=1, column=0, sticky="w", pady=5)
        self.output_folder_var = tk.StringVar()
        self.output_folder_entry = ttk.Entry(output_options_frame, textvariable=self.output_folder_var, width=35, state=tk.DISABLED) 
        self.output_folder_entry.grid(row=1, column=1, columnspan=4, padx=5, pady=5, sticky="ew")
        self.browse_output_folder_button = ttk.Button(output_options_frame, text="Browse Folder", command=self.browse_output_folder, state=tk.DISABLED) 
        self.browse_output_folder_button.grid(row=1, column=5, padx=5, pady=5) 

        # Logo Label (replaces the logo_frame and now directly displays the image)
        logo_size = 100 
        self.logo_label = ttk.Label(main_container_frame, anchor="center")
        self.logo_label.pack(side="right", anchor="ne", padx=10, pady=5)
        
        # Load and display logo, resizing to fit the square area
        if self.logo_image_path and os.path.exists(self.logo_image_path):
            try:
                original_image = Image.open(self.logo_image_path)
                resized_image = original_image.resize((logo_size, logo_size), Image.LANCZOS) 
                self.logo_tk = ImageTk.PhotoImage(resized_image)
                self.logo_label.config(image=self.logo_tk)
            except Exception as e:
                self.logo_label.config(text=f"Error loading logo: {e}", background="red", foreground="white") 
                self.log_queue.put(f"Error loading logo from '{self._format_path_for_display(self.logo_image_path)}': {e}\n")

        # End Logo Handling

        # Buttons Frame (remains below the main_container_frame)
        buttons_frame = ttk.Frame(self)
        buttons_frame.pack(pady=10)

        self.run_button = ttk.Button(buttons_frame, text="Run Lookup", command=self.run_lookup_in_thread)
        self.run_button.pack(side="left", padx=5)

        self.save_log_button = ttk.Button(buttons_frame, text="Save Console Log", command=self.save_log, state=tk.DISABLED)
        self.save_log_button.pack(side="left", padx=5)

        # Output Text Area with Scrollbar (remains below buttons_frame)
        output_text_frame = ttk.Frame(self)
        output_text_frame.pack(padx=10, pady=10, fill="both", expand=True)

        self.output_text = tk.Text(output_text_frame, wrap="word", height=20, width=80)
        self.output_text.pack(side="left", fill="both", expand=True)

        self.scrollbar = ttk.Scrollbar(output_text_frame, command=self.output_text.yview)
        self.scrollbar.pack(side="right", fill="y")

        self.output_text.config(yscrollcommand=self.scrollbar.set)
        
        self.output_text.insert(tk.END, f"{app_name} {version}\nhttps://github.com/stark4n6/asp-search\n\n")

        # Initialize the state of output folder widgets based on default output format
        self.on_output_format_change()

    def on_output_format_change(self, *args):
        """
        Enables or disables the output folder Browse widgets based on the selected
        output format (console vs. file/db).
        """
        current_format = self.output_format_var.get()
        if current_format == 'console':
            self.browse_output_folder_button.config(state=tk.DISABLED)
            # Temporarily enable to clear, then disable
            self.output_folder_entry.config(state=tk.NORMAL) 
            self.output_folder_var.set("")
            self.output_folder_entry.config(state=tk.DISABLED)
        else:
            self.browse_output_folder_button.config(state=tk.NORMAL)
            self.output_folder_entry.config(state=tk.DISABLED) # Keep entry disabled for manual input

    def browse_file(self):
        """
        Opens a file dialog for the user to select an input ID file.
        """
        file_path = filedialog.askopenfilename(
            title="Select Input ID File",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        if file_path:
            self.input_id_entry.delete(0, tk.END)
            self.input_id_entry.insert(0, file_path)

    def browse_output_folder(self):
        """
        Opens a directory dialog for the user to select an output folder.
        """
        folder_selected = filedialog.askdirectory(title="Select Output Folder")
        if folder_selected:
            # Temporarily enable to set value, then disable
            self.output_folder_entry.config(state=tk.NORMAL) 
            self.output_folder_var.set(folder_selected)
            self.output_folder_entry.config(state=tk.DISABLED) 

    def run_lookup_in_thread(self):
        """
        Initiates the lookup process in a separate thread to keep the GUI responsive.
        """
        self.output_text.delete(1.0, tk.END) # Clear previous output
        
        # Disable buttons during lookup
        self.run_button.config(state=tk.DISABLED)
        self.save_log_button.config(state=tk.DISABLED)

        self.actual_output_dir = None # Reset actual output directory

        # Start the lookup in a new thread
        thread = threading.Thread(target=self._run_lookup)
        thread.start()

    def _run_lookup(self):
        """
        Contains the core logic for fetching, parsing, and saving data.
        This method runs in a separate thread.
        """
        input_id_value = self.input_id_entry.get()
        lookup_type = self.lookup_type_var.get()
        output_format = self.output_format_var.get()
        selected_output_directory = self.output_folder_var.get()

        if not input_id_value:
            self.log_queue.put("ERROR: Please provide an AdamID/BundleID or a file path.\n")
            self.after(100, lambda: self.run_button.config(state=tk.NORMAL))
            self.after(100, lambda: self.save_log_button.config(state=tk.NORMAL))
            return

        script = "asp-search"
        start_time = datetime.now()
        time_format_filename = "%Y%m%d_%H%M%S"
        
        # --- Start: Console Header (Always print to GUI console) ---
        self.log_queue.put(f"{app_name} {version}\n")
        self.log_queue.put(f"https://github.com/stark4n6/asp-search\n")
        self.log_queue.put(f"--- Lookup Started ---\n")
        self.log_queue.put(f"Start: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        # --- End: Console Header ---


        # Handle output directory creation for file/db formats
        if (output_format == 'txt' or output_format == 'db' or output_format == 'both'):
            # Determine the base output directory (selected by user or current working directory)
            base_output_dir = selected_output_directory if selected_output_directory and os.path.isdir(selected_output_directory) else os.getcwd()

            # Create the timestamped output subfolder
            timestamped_folder_name = f"asp-search_out_{start_time.strftime(time_format_filename)}"
            self.actual_output_dir = os.path.join(base_output_dir, timestamped_folder_name)
            
            try:
                os.makedirs(self.actual_output_dir, exist_ok=True) # Create the directory if it doesn't exist
                self.log_queue.put(f"Output folder created at: {self._format_path_for_display(self.actual_output_dir)}\n")
            except Exception as e:
                self.log_queue.put(f"ERROR: Could not create output folder '{self._format_path_for_display(self.actual_output_dir)}': {e}. Falling back to console output.\n")
                self.actual_output_dir = None # Reset to prevent file writing if folder creation failed
                output_format = 'console' # Fallback to console if folder creation fails

        output_filename = None
        database_filename = None
        if self.actual_output_dir:
            # Construct full paths for output files within the new timestamped folder
            output_filename = os.path.join(self.actual_output_dir, f"{script}_output_{start_time.strftime(time_format_filename)}.txt")
            database_filename = os.path.join(self.actual_output_dir, f"{script}_output_{start_time.strftime(time_format_filename)}.db")

        table_name = "app_bundle_data"

        report_output_stream = None
        conn = None
        cursor = None

        # Setup text file output stream
        if output_format == 'txt' or output_format == 'both':
            if output_filename:
                try:
                    report_output_stream = open(output_filename, "w+")
                    self.log_queue.put(f"Text output will be saved to {self._format_path_for_display(output_filename)}\n\n")
                    # Write initial report headers to the text file output stream
                    report_output_stream.write(f"{app_name} {version}\nhttps://github.com/stark4n6/asp-search\n--- Lookup Started ---\n")
                    report_output_stream.write(f"Start: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                except IOError as e:
                    self.log_queue.put(f"ERROR: Could not open text file for writing: {e}\n")
                    report_output_stream = None # Indicate that file writing failed
            else:
                self.log_queue.put("ERROR: Text output filename not determined. Skipping text file output.\n")
                report_output_stream = None # No file output if filename not determined

        # Setup SQLite database connection
        if output_format == 'db' or output_format == 'both':
            if database_filename:
                try:
                    conn = sqlite3.connect(database_filename)
                    cursor = conn.cursor()

                    # Create metadata table if it doesn't exist
                    cursor.execute(f'''
                        CREATE TABLE IF NOT EXISTS {METADATA_TABLE_NAME} (
                            key TEXT PRIMARY KEY,
                            value TEXT
                        )
                    ''')
                    conn.commit()

                    # Insert/Update header details into metadata table
                    current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    metadata_items = {
                        "AppName": app_name,
                        "Version": version,
                        "Source": "https://github.com/stark4n6/asp-search",
                        "LookupStartTime": current_time_str,
                        "LookupType": lookup_type,
                        "InputIDValue": input_id_value,
                    }

                    for key, value in metadata_items.items():
                        cursor.execute(f"INSERT OR REPLACE INTO {METADATA_TABLE_NAME} (key, value) VALUES (?, ?)", (key, value))
                    conn.commit()
                    self.log_queue.put(f"Metadata (header details) stored in '{METADATA_TABLE_NAME}' table.\n")


                    # Check and reorder/create app_bundle_data table if schema mismatch or table doesn't exist
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    existing_columns_info = cursor.fetchall()
                    existing_column_names = [info[1] for info in existing_columns_info]

                    needs_reorder = False
                    if not existing_column_names:
                        needs_reorder = True
                    elif len(existing_columns_info) != len(DESIRED_COLUMN_ORDER):
                        needs_reorder = True
                    else:
                        for i, col in enumerate(DESIRED_COLUMN_ORDER):
                            if existing_column_names[i] != col:
                                needs_reorder = True
                                break

                    if needs_reorder:
                        #self.log_queue.put(f"Schema mismatch detected or table '{table_name}' does not exist. Reordering/creating table with desired column order...\n")
                        err, success = create_and_reorder_table(conn, cursor, table_name, DESIRED_COLUMN_ORDER, existing_column_names)
                        if not success:
                            self.log_queue.put(f"Failed to reorder/create database table: {err}. Skipping database output.\n")
                            if conn: conn.close()
                            conn = None 
                            cursor = None
                    
                    if conn:
                        self.log_queue.put(f"Database '{self._format_path_for_display(database_filename)}' opened/created. Table '{table_name}' ensured.\n\n")

                except sqlite3.Error as e:
                    self.log_queue.put(f"SQLite error during database setup: {e}. Skipping database output.\n")
                    if conn: conn.close()
                    conn = None 
                    cursor = None
            else:
                self.log_queue.put("ERROR: Database filename not determined. Skipping database output.\n")

        # Get the list of IDs to process (and deduplicate)
        error, input_id_list = set_input_id_list(input_id_value)
        if error:
            self.log_queue.put(f"ERROR: {error}\n")
            self.after(100, lambda: self.run_button.config(state=tk.NORMAL))
            self.after(100, lambda: self.save_log_button.config(state=tk.NORMAL))
            self.after(200, self.show_completion_popup)
            return
        
        if not input_id_list:
            self.log_queue.put("No valid IDs found to process after deduplication (if applicable).\n")
            self.after(100, lambda: self.run_button.config(state=tk.NORMAL))
            self.after(100, lambda: self.save_log_button.config(state=tk.NORMAL))
            self.after(200, self.show_completion_popup)
            return

        processed_results_for_output = {}
        total_unique_ids = len(input_id_list) # This is the total number of unique IDs

        # Process each ID
        for i, current_id in enumerate(input_id_list):
            current_lookup_num = i + 1
            self.log_queue.put(f"Processing ID: {current_id} ({current_lookup_num}/{total_unique_ids})\n")
            err, bundleID_data = get_data_from_itunes(current_id, lookup_type)
            if err:
                self.log_queue.put(err + "\n")
                # Create a placeholder entry for failed lookups
                if lookup_type == "adamId":
                    parsed_results = {"adamId": current_id, "bundleId": "N/A", "error_message": err}
                else:
                    parsed_results = {"adamId": "N/A", "bundleId": current_id, "error_message": err}
                
                # Use current_id as the key for failed lookups in processed_results_for_output
                processed_results_for_output[current_id] = parsed_results

            elif bundleID_data is not None:
                flat_parsed_data = parse_itunes_data(bundleID_data, PARSING_KEYS, current_id, lookup_type)
                
                # Determine the key for output dictionary based on lookup type or actual AdamId/BundleId
                if lookup_type == "adamId":
                    # For AdamID lookup, use AdamID from the parsed data
                    output_key = flat_parsed_data.get("adamId", current_id)
                else: # lookup_type == "bundleId"
                    # For BundleID lookup, use BundleID from the parsed data
                    # If found, it will have adamId, if not, it will have the original bundleId
                    output_key = flat_parsed_data.get("bundleId", current_id)

                if output_key == "N/A" or output_key is None:
                    # Fallback to the original lookup ID if both adamId and bundleId are N/A
                    output_key = current_id 
                    
                processed_results_for_output[output_key] = flat_parsed_data
                
                # Insert/update data in SQLite database if connection is active
                if conn and cursor: 
                    db_adam_id_for_pk = flat_parsed_data.get("adamId")
                    if db_adam_id_for_pk == "N/A" or db_adam_id_for_pk is None:
                        # If no adamId, use a unique identifier for the primary key
                        db_adam_id_for_pk = f"NO_ADAMID_{lookup_type}_{current_id}"
                        flat_parsed_data["adamId"] = db_adam_id_for_pk # Ensure adamId is set for PK

                    columns = []
                    values = []
                    placeholders = []

                    for col in DESIRED_COLUMN_ORDER:
                        columns.append(col)
                        values.append(flat_parsed_data.get(col, None))
                        placeholders.append("?")

                    insert_sql = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

                    try:
                        cursor.execute(insert_sql, tuple(values))
                        conn.commit()
                    except sqlite3.Error as e:
                        self.log_queue.put(f"Error inserting data for {db_adam_id_for_pk}: {e}\n")
                        
            else:
                self.log_queue.put(f"Skipping processing and output for {current_id}: Failed to fetch data from iTunes API (unknown error).\n")

        # Write processed results to the text output stream (file)
        if report_output_stream: # This will be true only if 'txt' or 'both' and file was successfully opened
            for key_for_output_dict, data_to_write in processed_results_for_output.items():
                display_id_for_header = key_for_output_dict # Use the dictionary key which should be the AdamId or original BundleId
                
                report_output_stream.write(f"--- Data for {lookup_type}: {display_id_for_header} ---\n")

                for col in DESIRED_COLUMN_ORDER:
                    value = data_to_write.get(col)
                    if value is not None:
                        report_output_stream.write(f"{col}: {value}\n")
                    else:
                        report_output_stream.write(f"{col}: N/A\n") # Ensure all columns are present

                report_output_stream.write("\n") # Add a blank line for readability

            end_time = datetime.now()
            duration = end_time - start_time
            report_output_stream.write(f"--- Lookup Finished ---\n")
            report_output_stream.write(f"Total time taken: {duration}\n")
            report_output_stream.write(f"Timestamp: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

        # Write processed results to the console output (GUI Text widget)
        # This block now handles console output for both AdamID and BundleID uniformly
        # This will always execute for displaying results in the GUI console
        for key_for_output_dict, data_to_write in processed_results_for_output.items():
            display_id_for_header = key_for_output_dict # Use the dictionary key which should be the AdamId or original BundleId
            
            self.log_queue.put(f"--- Data for {lookup_type}: {display_id_for_header} ---\n")

            for col in DESIRED_COLUMN_ORDER:
                value = data_to_write.get(col)
                if value is not None:
                    self.log_queue.put(f"{col}: {value}\n")
                else:
                    self.log_queue.put(f"{col}: N/A\n") # Ensure all columns are present

            self.log_queue.put("\n") # Add a blank line for readability

        # Add end timestamp and duration to console output (ALWAYS ONCE)
        end_time = datetime.now()
        duration = end_time - start_time
        self.log_queue.put(f"--- Lookup Finished ---\n")
        self.log_queue.put(f"Total time taken: {duration}\n")
        self.log_queue.put(f"Timestamp: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        self.log_queue.put("Lookup process completed.\n")

        # Update metadata table with end time and duration
        if conn and cursor:
            try:
                end_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                duration_str = str(duration)
                cursor.execute(f"INSERT OR REPLACE INTO {METADATA_TABLE_NAME} (key, value) VALUES (?, ?)", ("LookupEndTime", end_time_str))
                cursor.execute(f"INSERT OR REPLACE INTO {METADATA_TABLE_NAME} (key, value) VALUES (?, ?)", ("TotalDuration", duration_str))
                conn.commit()
                self.log_queue.put(f"\nFinal metadata (end time, duration) stored in '{METADATA_TABLE_NAME}' table.\n")
            except sqlite3.Error as e:
                self.log_queue.put(f"Error updating metadata table with end details: {e}\n")


        # Close connections and streams
        if report_output_stream: # This will be true only if 'txt' or 'both' and file was successfully opened
            try:
                report_output_stream.close()
                self.log_queue.put(f"Text output saved to: {self._format_path_for_display(output_filename)}\n")
            except Exception as e:
                self.log_queue.put(f"ERROR: Could not close text file: {e}\n")
        if conn:
            try:
                conn.close()
                self.log_queue.put(f"Database saved to: {self._format_path_for_display(database_filename)}\n")
            except Exception as e:
                self.log_queue.put(f"ERROR: Could not close database: {e}\n")

        
        # Re-enable buttons after lookup is complete
        self.after(100, lambda: self.run_button.config(state=tk.NORMAL))
        self.after(100, lambda: self.save_log_button.config(state=tk.NORMAL))
        self.after(200, self.show_completion_popup) # Call the modified completion popup


    def save_log(self):
        """
        Saves the content of the console output text widget to a file.
        """
        if self.actual_output_dir:
            default_filename = os.path.join(self.actual_output_dir, f"console_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        else:
            default_filename = f"console_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

        file_path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            initialfile=os.path.basename(default_filename),
            initialdir=os.path.dirname(default_filename) if self.actual_output_dir else os.getcwd(),
            title="Save Console Log",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        if file_path:
            try:
                with open(file_path, "w") as f:
                    f.write(self.output_text.get(1.0, tk.END))
                messagebox.showinfo("Success", f"Console log saved to:\n{self._format_path_for_display(file_path)}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save console log:\n{e}")

    def open_output_folder(self):
        """
        Opens the generated output folder in the system's file explorer.
        """
        if self.actual_output_dir and os.path.isdir(self.actual_output_dir):
            try:
                if sys.platform == "win32":
                    os.startfile(self.actual_output_dir)
                elif sys.platform == "darwin": # macOS
                    subprocess.Popen(["open", self.actual_output_dir])
                else: # linux variants
                    subprocess.Popen(["xdg-open", self.actual_output_dir])
                self.log_queue.put(f"Opened output folder: {self._format_path_for_display(self.actual_output_dir)}\n")
            except Exception as e:
                self.log_queue.put(f"ERROR: Could not open output folder '{self._format_path_for_display(self.actual_output_dir)}': {e}\n")
                messagebox.showerror("Error", f"Could not open output folder.\nError: {e}")
        else:
            messagebox.showinfo("Info", "No valid output folder to open.")

    def show_completion_popup(self):
        """
        Displays a popup message upon completion of the lookup process and asks
        if the user wants to open the output folder.
        """
        messagebox.showinfo("Lookup Complete", "The lookup process has finished.")
        
        # Only ask to open the folder if one was successfully created
        if self.actual_output_dir and os.path.isdir(self.actual_output_dir):
            should_open = messagebox.askyesno(
                "Open Output Folder?",
                "Would you like to open the generated output folder?"
            )
            if should_open:
                self.open_output_folder()


if __name__ == "__main__":
    app = App()
    app.mainloop()