import os

class Prompts:
    def __init__(self):
        self.prompts_dir = "prompts"
        self._load_prompts()

    def _load_prompts(self):
        for filename in os.listdir(self.prompts_dir):
            if filename.endswith(".txt"):
                prompt_name = os.path.splitext(filename)[0]
                file_path = os.path.join(self.prompts_dir, filename)
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as file:
                        prompt_content = file.read().strip()
                        setattr(self, prompt_name, prompt_content)
                except FileNotFoundError:
                    print(f"Error: File not found at {file_path}")
                except IOError:
                    print(f"Error: Unable to read file at {file_path}")

prompts = Prompts()

# Usage example:
# from prompts import prompts
# print(prompts.example_prompt)  # Assuming there's a file named "example_prompt.txt" in the prompts folder