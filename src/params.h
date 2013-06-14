
#ifndef PARAMS_H
#define PARAMS_H

bool Params_init();

extern PyObject* SQLParameter_type;

struct Cursor;

bool BindParams(Cursor* cur, PyObject* params, bool skip_first);
bool PrepareAndBind(Cursor* cur, PyObject* pSql, PyObject* params, bool skip_first);
void FreeParameterData(Cursor* cur);
void FreeParameterInfo(Cursor* cur);

#endif
