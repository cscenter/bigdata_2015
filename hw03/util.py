import base64

def encode_term(term):
	return base64.b64encode(term, "_-")

def decode_term(encoded):
	return base64.b64decode(encoded, "_-")

def scorpion_decode(s):
	ret = s[0:min(len(s), 3)]
	while len(ret) < 3:
		ret += '_'
	return ret