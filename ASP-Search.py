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

def set_input_id_list(set_input_id_items):
    input_id_list = []
    if os.path.exists(set_input_id_items):
        try:
            with open(set_input_id_items, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        input_id_list.append(line)
        except Exception as e:
            return f"error: {e}", []
    else:
        input_id_list = [set_input_id_items]
    return None, input_id_list

def get_data_from_itunes(lookup_value, lookup_type):
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
    parsed_results_flat = {}
    
    if "resultCount" in bundle_data:
        if bundle_data["resultCount"] == 0:
            if lookup_type == "adamId":
                parsed_results_flat["adamId"] = original_lookup_value
                parsed_results_flat["bundleId"] = "N/A"
            elif lookup_type == "bundleId":
                parsed_results_flat["adamId"] = "N/A"
                parsed_results_flat["bundleId"] = original_lookup_value

            parsed_results_flat["error_message"] = f"No data found at itunes.apple.com for: {original_lookup_value} (lookup by {lookup_type})"
        else:
            data = (bundle_data["results"][0])
            
            if lookup_type == "adamId":
                parsed_results_flat["adamId"] = original_lookup_value
            elif lookup_type == "bundleId" and "trackId" in data:
                parsed_results_flat["adamId"] = str(data["trackId"])
            else:
                parsed_results_flat["adamId"] = "N/A"

            for k, v in data.items():
                if k == "trackId" and lookup_type == "bundleId":
                    continue
                if k == "bundleId": 
                    parsed_results_flat["bundleId"] = v
                    continue 

                if k in parsing_keys_list:
                    parsed_results_flat[k] = v
    return parsed_results_flat

def create_and_reorder_table(conn, cursor, table_name, desired_order, existing_columns):
    temp_table_name = f"{table_name}_temp"

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

    cursor.execute(f"PRAGMA table_info({table_name})")
    old_table_info = cursor.fetchall()
    old_column_names = [info[1] for info in old_table_info]

    column_mapping = {
        "bundle_id_lookup": "adamId",
    }

    columns_to_copy_select = []
    columns_to_copy_insert = []

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

    drop_old_table_sql = f"DROP TABLE IF EXISTS {table_name}"
    try:
        cursor.execute(drop_old_table_sql)
        conn.commit()
    except sqlite3.Error as e:
        return f"Error dropping old table: {e}", False

    rename_table_sql = f"ALTER TABLE {temp_table_name} RENAME TO {table_name}"
    try:
        cursor.execute(rename_table_sql)
        conn.commit()
    except sqlite3.Error as e:
        return f"Error renaming table: {e}", False

    return None, True

class TextRedirector:
    def __init__(self, widget, q):
        self.widget = widget
        self.q = q

    def write(self, str_val):
        self.q.put(str_val)

    def flush(self):
        pass

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(f"ASP Search - App Store Package Search v0.1")
        self.resizable(False, False) 
        self.geometry("750x650") 

        self.actual_output_dir = None
        self.logo_tk = None 

        self.logo_image_path = "aspis.png" 

        self.log_queue = queue.Queue() 

        self.create_widgets()
        self.create_menu() # Call the new method to create the menu
        
        self.process_queue() 
        sys.stdout = TextRedirector(self.output_text, self.log_queue)
        sys.stderr = TextRedirector(self.output_text, self.log_queue)

    def _format_path_for_display(self, path):
        """Converts a given path to use forward slashes for display."""
        if path:
            return path.replace(os.path.sep, '/')
        return path

    def process_queue(self):
        while not self.log_queue.empty():
            line = self.log_queue.get_nowait()
            self.output_text.insert(tk.END, line)
            self.output_text.see(tk.END)
        self.after(100, self.process_queue)

    def create_menu(self):
        # Create the main menu bar
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
            webbrowser.open_new("https://github.com/stark4n6")
        except Exception as e:
            self.log_queue.put(f"ERROR: Could not open GitHub link: {e}\n")
            messagebox.showerror("Error", f"Failed to open GitHub link. Please visit https://github.com/stark4n6 manually.\nError: {e}")


    def create_widgets(self):
        # Main container for left panel (input/output) and right panel (logo)
        main_container_frame = tk.Frame(self)
        main_container_frame.pack(fill="x", padx=10, pady=10) 

        # Left Panel: Input and Output Options
        left_panel_frame = ttk.Frame(main_container_frame)
        left_panel_frame.pack(side="left", anchor="nw", expand=False) 

        # Input Frame - Now inside left_panel_frame
        input_frame = ttk.LabelFrame(left_panel_frame, text="Input Details", padding="10")
        input_frame.pack(padx=5, pady=5, fill="x", anchor="nw") 
        input_frame.grid_columnconfigure(1, weight=1) 

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
                self.log_queue.put("Logo loaded successfully.\n")
            except Exception as e:
                self.logo_label.config(text=f"Error loading logo: {e}", background="red", foreground="white") 
                self.log_queue.put(f"Error loading logo from '{self._format_path_for_display(self.logo_image_path)}': {e}\n")
        else:
            self.logo_label.config(text="[Your Logo Here]", background="blue", foreground="white")
            self.log_queue.put("Logo path is empty or file does not exist. Displaying placeholder text.\n")
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
        
        self.output_text.insert(tk.END, f"ASP Search (App Store Package Search) v0.1\nhttps://github.com/stark4n6\n\n")

        self.on_output_format_change()

    def on_output_format_change(self, *args):
        current_format = self.output_format_var.get()
        if current_format == 'console':
            self.browse_output_folder_button.config(state=tk.DISABLED)
            self.output_folder_entry.config(state=tk.NORMAL) 
            self.output_folder_var.set("")
            self.output_folder_entry.config(state=tk.DISABLED)
        else:
            self.browse_output_folder_button.config(state=tk.NORMAL)
            self.output_folder_entry.config(state=tk.DISABLED)

    def browse_file(self):
        file_path = filedialog.askopenfilename(
            title="Select Input ID File",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        if file_path:
            self.input_id_entry.delete(0, tk.END)
            self.input_id_entry.insert(0, file_path)

    def browse_output_folder(self):
        folder_selected = filedialog.askdirectory(title="Select Output Folder")
        if folder_selected:
            self.output_folder_entry.config(state=tk.NORMAL) 
            self.output_folder_var.set(folder_selected)
            self.output_folder_entry.config(state=tk.DISABLED) 

    def run_lookup_in_thread(self):
        self.output_text.delete(1.0, tk.END)
        self.output_text.insert(tk.END, "Starting lookup...\n")
        
        self.run_button.config(state=tk.DISABLED)
        self.save_log_button.config(state=tk.DISABLED)

        self.actual_output_dir = None 

        thread = threading.Thread(target=self._run_lookup)
        thread.start()

    def _run_lookup(self):
        input_id_value = self.input_id_entry.get()
        lookup_type = self.lookup_type_var.get()
        output_format = self.output_format_var.get()
        selected_output_directory = self.output_folder_var.get()

        if not input_id_value:
            self.log_queue.put("ERROR: Please provide an AdamID/BundleID or a file path.\n")
            self.after(100, lambda: self.run_button.config(state=tk.NORMAL))
            self.after(100, lambda: self.save_log_button.config(state=tk.NORMAL))
            return

        script = "adamID_bundleID_lookup"
        version = "v0.1"
        start_time = datetime.now()
        time_format_filename = "%Y%m%d_%H%M%S"
        
        if (output_format == 'txt' or output_format == 'db' or output_format == 'both'):
            # Determine the base output directory
            base_output_dir = selected_output_directory if selected_output_directory and os.path.isdir(selected_output_directory) else os.getcwd()

            # Create the timestamped output subfolder
            timestamped_folder_name = f"ASPS_output_{start_time.strftime(time_format_filename)}"
            self.actual_output_dir = os.path.join(base_output_dir, timestamped_folder_name)
            
            try:
                os.makedirs(self.actual_output_dir, exist_ok=True)
                self.log_queue.put(f"Output folder created at: {self._format_path_for_display(self.actual_output_dir)}\n")
            except Exception as e:
                self.log_queue.put(f"ERROR: Could not create output folder '{self._format_path_for_display(self.actual_output_dir)}': {e}. Falling back to console output.\n")
                self.actual_output_dir = None # Reset to prevent file writing if folder creation failed
                output_format = 'console' # Fallback to console if folder creation fails

        output_filename = None
        database_filename = None
        if self.actual_output_dir:
            output_filename = os.path.join(self.actual_output_dir, f"{script}_output_{start_time.strftime(time_format_filename)}.txt")
            database_filename = os.path.join(self.actual_output_dir, f"{script}_output_{start_time.strftime(time_format_filename)}.db")

        table_name = "app_bundle_data"

        report_output_stream = None
        conn = None
        cursor = None

        if output_format == 'txt' or output_format == 'both':
            if output_filename:
                try:
                    report_output_stream = open(output_filename, "w+")
                    self.log_queue.put(f"Text output will be saved to {self._format_path_for_display(output_filename)}\n")
                except IOError as e:
                    self.log_queue.put(f"ERROR: Could not open text file for writing: {e}\n")
                    report_output_stream = None
            else:
                self.log_queue.put("ERROR: Text output filename not determined. Falling back to console.\n")
                report_output_stream = sys.stdout
        elif output_format == 'console':
            report_output_stream = sys.stdout

        if report_output_stream:
            report_output_stream.write(f"{script} {version} results\n")
            report_output_stream.write(f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        if output_format == 'db' or output_format == 'both':
            if database_filename:
                try:
                    conn = sqlite3.connect(database_filename)
                    cursor = conn.cursor()

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
                        self.log_queue.put(f"Schema mismatch detected or table '{table_name}' does not exist. Reordering/creating table with desired column order...\n")
                        err, success = create_and_reorder_table(conn, cursor, table_name, DESIRED_COLUMN_ORDER, existing_column_names)
                        if not success:
                            self.log_queue.put(f"Failed to reorder/create database table: {err}. Skipping database output.\n")
                            if conn: conn.close()
                            conn = None 
                            cursor = None
                    
                    if conn:
                        self.log_queue.put(f"Database '{self._format_path_for_display(database_filename)}' opened/created. Table '{table_name}' ensured.\n")

                except sqlite3.Error as e:
                    self.log_queue.put(f"SQLite error during database setup: {e}. Skipping database output.\n")
                    if conn: conn.close()
                    conn = None 
                    cursor = None
            else:
                self.log_queue.put("ERROR: Database filename not determined. Skipping database output.\n")

        error, input_id_list = set_input_id_list(input_id_value)
        if error:
            self.log_queue.put(f"ERROR: {error}\n")
            self.after(100, lambda: self.run_button.config(state=tk.NORMAL))
            self.after(100, lambda: self.save_log_button.config(state=tk.NORMAL))
            self.after(200, self.show_completion_popup)
            return

        processed_results_for_output = {}

        for current_id in input_id_list:
            self.log_queue.put(f"Processing ID: {current_id}\n")
            err, bundleID_data = get_data_from_itunes(current_id, lookup_type)
            if err:
                self.log_queue.put(err + "\n")
                if lookup_type == "adamId":
                    parsed_results = {"adamId": current_id, "bundleId": "N/A", "error_message": err}
                else:
                    parsed_results = {"adamId": "N/A", "bundleId": current_id, "error_message": err}
                processed_results_for_output[f"LOOKUP_ERROR_{lookup_type}_{current_id}"] = parsed_results

            elif bundleID_data is not None:
                flat_parsed_data = parse_itunes_data(bundleID_data, PARSING_KEYS, current_id, lookup_type)
                
                text_output_key = flat_parsed_data.get("adamId")
                if text_output_key == "N/A" or text_output_key is None:
                    text_output_key = f"LOOKUP_ERROR_{lookup_type}_{current_id}"
                processed_results_for_output[text_output_key] = flat_parsed_data
                
                if conn and cursor: 
                    db_adam_id_for_pk = flat_parsed_data.get("adamId")
                    if db_adam_id_for_pk == "N/A" or db_adam_id_for_pk is None:
                        db_adam_id_for_pk = f"LOOKUP_FAIL_{lookup_type}_{current_id}"
                        flat_parsed_data["adamId"] = db_adam_id_for_pk

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


        if report_output_stream: 
            header_id_type = "adamId" if lookup_type == "adamId" else "bundleId"

            for key_for_output_dict, data_to_write in processed_results_for_output.items():
                display_id_for_header = None
                if header_id_type == "adamId":
                    display_id_for_header = data_to_write.get("adamId")
                    if display_id_for_header == "N/A" or display_id_for_header is None:
                        if key_for_output_dict.startswith("LOOKUP_ERROR_adamId_"):
                            display_id_for_header = key_for_output_dict.replace("LOOKUP_ERROR_adamId_", "")
                        elif key_for_output_dict.startswith("LOOKUP_FAIL_adamId_"):
                            display_id_for_header = key_for_output_dict.replace("LOOKUP_FAIL_adamId_", "")
                        else:
                            display_id_for_header = key_for_output_dict
                else:
                    display_id_for_header = data_to_write.get("bundleId")
                    if display_id_for_header == "N/A" or display_id_for_header is None:
                        if key_for_output_dict.startswith("LOOKUP_ERROR_bundleId_"):
                            display_id_for_header = key_for_output_dict.replace("LOOKUP_ERROR_bundleId_", "")
                        elif key_for_output_dict.startswith("LOOKUP_FAIL_bundleId_"):
                            display_id_for_header = key_for_output_dict.replace("LOOKUP_FAIL_bundleId_", "")
                        else:
                            display_id_for_header = key_for_output_dict

                report_output_stream.write(f"--- Data for {header_id_type}: {display_id_for_header} ---\n")

                for col in DESIRED_COLUMN_ORDER:
                    value = data_to_write.get(col)
                    if value is not None:
                        if col == "error_message":
                            report_output_stream.write(f"{value}\n")
                        else:
                            report_output_stream.write(f"{col}: {value}\n")
                report_output_stream.write("\n")

            if report_output_stream != sys.stdout:
                report_output_stream.close()
                self.log_queue.put(f"Text output saved to {self._format_path_for_display(output_filename)}\n")

        if conn:
            conn.close()
            self.log_queue.put(f"Database '{self._format_path_for_display(database_filename)}' closed.\n")

        self.log_queue.put("Lookup process completed.\n")
        self.after(100, lambda: self.run_button.config(state=tk.NORMAL)) 
        self.after(100, lambda: self.save_log_button.config(state=tk.NORMAL))
        
        self.after(200, self.show_completion_popup)

    def show_completion_popup(self):
        current_format = self.output_format_var.get()
        if current_format != 'console' and self.actual_output_dir and os.path.isdir(self.actual_output_dir):
            response = messagebox.askyesno(
                "Lookup Complete",
                f"Lookup process completed.\n\nOutput saved to:\n{self._format_path_for_display(self.actual_output_dir)}\n\nDo you want to open the output folder?"
            )
            if response:
                self.open_output_folder(self.actual_output_dir)
        elif current_format == 'console':
            messagebox.showinfo("Lookup Complete", "Lookup process completed. Output displayed in console.")
        else:
            messagebox.showinfo("Lookup Complete", "Lookup process completed. Output files may not have been saved due to an invalid path.")

    def open_output_folder(self, path):
        try:
            if sys.platform == "win32":
                os.startfile(path)
            elif sys.platform == "darwin":
                subprocess.Popen(["open", path])
            else:
                subprocess.Popen(["xdg-open", path])
        except Exception as e:
            messagebox.showerror("Error Opening Folder", f"Could not open folder:\n{self._format_path_for_display(path)}\nError: {e}")

    def save_log(self):
        file_path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            title="Save Console Log As",
            initialfile=f"console_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )
        if file_path:
            try:
                with open(file_path, "w") as f:
                    log_content = self.output_text.get("1.0", tk.END)
                    f.write(log_content)
                messagebox.showinfo("Save Log", f"Console log saved successfully to:\n{self._format_path_for_display(file_path)}")
            except Exception as e:
                messagebox.showerror("Save Log Error", f"Failed to save console log:\n{e}")

if __name__ == "__main__":
    app = App()
    app.mainloop()
