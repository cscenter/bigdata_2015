import base64

DEF_DELIM = " "


def encode_term(term):
    return base64.b64encode(term, "_-")


def decode_term(encoded):
    return base64.b64decode(encoded, "_-")


def arg_to_str(delimiter, *args):
    return delimiter.join([str(i) for i in list(args)])


def str_to_arg(delimiter, args_str, schema):
    schema_types = schema.strip().split(' ')
    if schema_types:
        args_str_list = args_str.strip().split(delimiter)
        if args_str_list:
            result = []
            if len(args_str_list) == len(schema_types):
                for arg, schema_type in zip(args_str_list, schema_types):
                    try:
                        if schema_type == "int":
                            result.append(int(arg))
                        elif schema_type == "float":
                            result.append(float(arg))
                        elif schema_type == "str":
                            result.append(str(arg))
                        else:
                            raise Exception("Unsupported type in schema: %s" % schema_type)
                    except ValueError:
                        raise Exception("cant convert arg: %s to type: %s in string: %s" % (arg, schema_type, args_str))
                if len(result) == 1:
                    return result[0]
                else:
                    return tuple(result)
            else:
                raise Exception("Amount of types in schema \"%s\" dose't fit for data: %s" % (schema, args_str))
        else:
            raise Exception("Empty str")
    else:
        raise Exception("Empty schema")


def append_to_str_of_tuple(delimiter, args_str, arg):
    if args_str:
        return args_str + delimiter + str(arg)
    else:
        return str(arg)


def prepend_to_str_of_tuple(delimiter, args_str, arg):
    if args_str:
        return str(arg) + delimiter + args_str
    else:
        return str(arg)

