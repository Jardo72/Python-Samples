#
# Copyright 2025 Jaroslav Chmurny
#
# This file is part of Python Samples.
#
# Python Samples is free software. It is licensed under the Apache License,
# Version 2.0 # (the "License"); you may not use this file except
# in compliance with the # License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

class DummyContextManager:

    def __init__(self, filename: str) -> None:
        self._filename = filename

    def __enter__(self):
        print("Entering the context...")
        return self

    def readlines(self):
        return [
            "This is just a dummy text file for testing purposes.",
            "Do not look for any deeper purpose in this text, it is here just to be read by the Python samples.",
            "Have a nice day, take care.",
        ]

    def __exit__(self, exc_type, exc_value, traceback):
        print("Exiting the context (closing resources...)")
        if exc_type:
            print(f"An exception occurred: {exc_value}")
            print(f"Exception type: {exc_type}")
            print("Exception traceback:")
            print(traceback)
        else:
            print("No exceptions occurred.")


def main():
    with DummyContextManager("test-file.txt") as cm:
        for line in cm.readlines():
            print(line.strip())


if __name__ == "__main__":
    main()
