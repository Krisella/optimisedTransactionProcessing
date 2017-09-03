#ifndef _LIST_HPP_
#define _LIST_HPP_

template <typename T>
struct node{
    node    *next;
    T       data;
};

template <typename T>
class List{
private:
    int     size;
    node<T> *root;
    node<T> *end;
public:
    List():size(0), root(NULL), end(NULL){};
    ~List(){};
    void deleteList();

    int push_back(T newData);
    int push_front(T newData);
    int pop(T *reqData);
    int peek(T *reqData);
    bool is_empty(){return size==0;};
    int get_size(){return size;};
    node<T> *get_root(){return root;};
};

template<typename T>
void List<T>::deleteList(){
    node<T> *toDelete, *cur;

    cur = root;
    while(cur != NULL){
        toDelete = cur;
        cur = cur->next;
        delete toDelete;
    }
}

template <typename T>
int List<T>::push_back(T newData){
    node<T> *newNode = new node<T>;

    newNode->next = NULL;
    newNode->data = newData;

    if(size == 0){
        root = newNode;
    }else{
        end->next = newNode;
    }
    end = newNode;
    size++;

    return 0;
}

template <typename T>
int List<T>::push_front(T newData){
    node<T> *newNode = new node<T>;

    newNode->next = root;
    newNode->data = newData;

    if(size == 0){
        end = newNode;
    }
    root = newNode;
    size++;

    return 0;
}

template <typename T>
int List<T>::pop(T *reqData){
    node<T> *toDelete;

    if(size == 0){
        return -1;
    }else{
        *reqData = root->data;
        toDelete = root;
        if(root->next == NULL) end = NULL;
        root = root->next;
        delete toDelete;
        size--;

        return 0;
    }
}

template <typename T>
int List<T>::peek(T *reqData){
    if(size == 0){
        return -1;
    }else{
        *reqData = root->data;

        return 0;
    }
}

#endif
