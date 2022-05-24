from transformers import pipeline


class Translate:
    """Class to translate from any language into English using a pre-trained Transformers model"""

    def translate_text(self, text: str) -> str:
        """Returns the translation of the text based on the pre-trained multilingual model"""
        try:
            translator = pipeline("translation", model=f"Helsinki-NLP/opus-mt-mul-en")
            translation = translator(text)
        except Exception as e:
            print(f"Exception while translating: {e}")
            translation = ""
        return translation
