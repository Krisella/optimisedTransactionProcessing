#include "structs.hpp"

int calculateOp_t(Op_t op,uint64_t value, uint64_t rec_value){

	switch(op){

	case Equal: return value == rec_value;

	case NotEqual: return value != rec_value;

	case Less: return rec_value < value;

	case LessOrEqual: return rec_value <= value;

	case Greater: return rec_value > value;

	case GreaterOrEqual: return rec_value >= value;

	default: return 0;
	}
};

