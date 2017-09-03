#ifndef _BSEARCH_HPP_
#define _BSEARCH_HPP_

/* Binary search algorithm. If unsigned then the return value must be checked
   for maximum value of the corresponding type. */
template <typename T, typename T1>
T1 binarySearch(T *array, T key, T1 start, T1 end){
    T1 left = 0, right = end;
    T1 mid = (start + end) / 2;
    while(right >= left){
        mid = (left + right) / 2;
        if(array[mid] == key){
            return mid;
        }else if(array[mid] < key){
            left = mid + 1;
        }else{
            right = mid - 1;
        }
    }

    return left;
}

template <typename T>
void quickSort(T *array, int start, int end){
    int p;
    if(start < end){
        p = partition(array, start, end);
        quickSort(array, start, p - 1);
        quickSort(array, p + 1, end);
    }
}

template <typename T>
int partition(T *array, int start, int end){
    T   pivot = array[end], temp;
    int i = start;

    for(int j = start; j < end; j++){
        if(array[j] <= pivot){
            temp = array[i];
            array[i] = array[j];
            array[j] = temp;
            i++;
        }
    }
    temp = array[i];
    array[i] = array[end];
    array[end] = temp;

    return i;
}

#endif
