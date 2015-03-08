import base64

def encode_term(term):
	return base64.b64encode(term, "_-")

def decode_term(encoded):
	return base64.b64decode(encoded, "_-")