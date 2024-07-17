import json
import os
import sys
from tqdm import tqdm
from copy import deepcopy
import pandas as pd




class QuestionDatasetEvaluator:
    def __init__(self):
        self._id2info = {"level1_source2_id_qita": {"clean_count":7497461, "ori_count": 7497461, "match_file": "3800w_testdata/match/level1_source2_id_qita_vegas_1100_300-10-1_clean_prompt_match.jsonl", "clean_file": "3800w_testdata/vegas_out_clean/level1_source2_id_qita_vegas_1100_300-10-1_clean.jsonl", "ori_file": "3800w_testdata/vegas_out_tpp_29_300/level1_source2_id_qita_vegas_1100_300-10-1_vegasOUT.jsonl"}}
        self._id2evalinfo = dict()

    def init(self):
        #TODO: Load id2info
        ori_path="jmjx_eval_total/t.xlsx"
        df = pd.read_excel(ori_path, sheet_name="原始")
        total_ori_count = 0
        total_clean_count = 0
        for i in range(df.shape[0]):
            id = df.iloc[i][0]
            source_name = df.iloc[i][1]
            ori_count = df.iloc[i][2]
            total_ori_count = ori_count

            clean_count = df.iloc[i][3]
            total_clean_count += clean_count

            file_prefix = df.iloc[i][5]
            ori_file_name = "t/{}_vegasOUT.jsonl".format(file_prefix)
            clean_file_name = "t/{}_clean.jsonl".format(file_prefix)
            match_file_name = "t/{}_clean_prompt_match.jsonl".format(file_prefix)
            self._id2info[id] = {"source_name": source_name, "ori_count": ori_count, "clean_count":clean_count, "match_file": match_file_name, "clean_file": clean_file_name, "ori_file": ori_file_name}

        # Calc source weight
        for id in self._id2info.keys():
            self._id2info[id]["clean_weight"] = float(self._id2info[id]["clean_count"]) / float(total_clean_count)
            self._id2info[id]["ori_weight"] = float(self._id2info[id]["ori_count"]) / float(total_ori_count)
        #Load id2evalinfo
        for id in self._id2info.keys():
            self._id2evalinfo[id] = self.load_dataset(id)

    def get_eval_dict_byid(self, id):
        if not id in self._id2evalinfo.keys():
            print ("cannot find ID: {}".format(id))
            return None
        return self._id2evalinfo[id]

    def load_jsonl(self, filename):
        lines = open(filename).readlines()
        res = dict()
        for l in lines:
            data = json.loads(l.strip())
            res[data["question_id"]] = data
        return res

    # when updating match file
    def load_new_match_file(self):
        new_path="jmjx_match_0709/1.xlsx"
        df = pd.read_excel(new_path, sheet_name="Sheet1")
        new_match_dic={}
        for i in range(df.shape[0]):
            id_=df.iloc[i][1]
            question_id=df.iloc[i][2]
            human_answer_label=df.iloc[i][4]
            human_match=df.iloc[i][7]
            if human_match=="正确":
                human_match="Y"
            else:
                human_match="N"
            v4_2_match=df.iloc[i][10]
            type_label=df.iloc[i][11]
            bak_label=df.iloc[i][12]
            if id_ not in new_match_dic:
                new_match_dic[id_]={}
            new_match_dic[id_][question_id]={}
            new_match_dic[id_][question_id]["human_answer_label"]=human_answer_label
            new_match_dic[id_][question_id]["human_match"]=human_match
            new_match_dic[id_][question_id]["v4_2_match"]=v4_2_match
            new_match_dic[id_][question_id]["type_label"]=type_label
            new_match_dic[id_][question_id]["bak_label"]=bak_label
            
        return new_match_dic
            


    def load_dataset(self, id):
        eval_info={}
        # eval_info={
        #     "id":id,
        #     'info':self._id2evalinfo[id],
        #     "clean_weight":self._id2info[id]["clean_weight"],
        #     "ori_weight":self._id2info[id]["ori_weight"],
        #     "v4_2_match":self._id2info[id]["v4_2_match"],
        # }
    # def load_dataset(self, id,add_by_qxy=None):
    #     print(add_by_qxy)
        if not id in self._id2info:
            print ("cannot find id: {}".format(id))
            sys.exit(1)
        eval_info = dict()
        # Get ori question id(All questions of a source, usually count is 300)
        ori_data = self.load_jsonl(self._id2info[id]["ori_file"])
        eval_info["ori_data"] = ori_data
        eval_info["ori_question_idset"] = set(ori_data.keys())

        # Get clean question id(filt problem questions)
        clean_data = self.load_jsonl(self._id2info[id]["clean_file"])
        eval_info["clean_data"] = clean_data
        eval_info["clean_question_idset"] = set(clean_data.keys())

        # Get match result(the count should be equal to clean questions)
        match_data = self.load_jsonl(self._id2info[id]["match_file"])
        assert (len(eval_info["clean_data"]) == len(match_data))
        
        # Load new match file
        match_data=self.load_new_match_file()
        match_data=match_data[id]
        
        eval_info["match_data"] = match_data
        eval_info["match_question_idset"]=set([q_id for q_id,v in match_data.items() if v["human_match"]=="Y"] )
        
        match_res = dict()
        for q_id in match_data.keys():
            match_res[q_id]=match_data[q_id]["human_match"]
            # match_res[q_id] = match_data[q_id]["v4.2_match"]
        eval_info["match_res"] = match_res
# >>>>>>> d1da36b... modify load_dataset function to add add_by_qxy
        return eval_info

    def calc_err_rate(self, err_count, all_count, is_show=True):
        res = {"err_rate": float(err_count)/float(all_count), "all_count": all_count, "err_count": err_count, "right_count": (all_count - err_count)}
        res["right_rate"] = float(res["right_count"]) / float(all_count)
        if is_show:
            print ("Error Rate: {}({}/{})".format(round(res["err_rate"], 4), err_count, all_count))
            print ("Right Rate: {}({}/{})".format(round(res["right_rate"], 4), res["right_count"], all_count))
        return res,["{}({}/{})".format(round(res["err_rate"], 4), err_count, all_count),"{}({}/{})".format(round(res["right_rate"], 4), res["right_count"], all_count)]
    
    # Calculate the percent of ineffective data
    def calc_ineffective(self,id_,filtered_qid_list):
        # 无效题目：10题，有效题目：10题
        # 过滤题目：6题（其中有效题目占3题，无效题目占3题）
        # ineffective percent = 6/20=0.3   
        # ineffective right percent = 3/6=0.5
        # effective err percent = 3/20=0.15
        source_id=id_
        ## 通过ID获取用于评估的信息（eval_dict)
        test_eval_dict =self.get_eval_dict_byid(source_id)
        ## 计算过滤前的错误/准确率
        question_err_rate_ori, answer_err_rate_ori, question_answer_err_rate_ori,_= self.analyze_by_evalinfo(test_eval_dict)
        ## 过滤questionid并重新计算错误/准确率
        test_filted_eval_dict = self.filter_by_question_id(source_id,filtered_qid_list)
        question_err_rate_filter, answer_err_rate_filter, question_answer_err_rate_filter,_= self.analyze_by_evalinfo(test_filted_eval_dict)
        # 计算无效题目&答案占比
        err_cnt_filter= question_answer_err_rate_ori['all_count']-question_answer_err_rate_filter['all_count']
        err_rate_percent=round(err_cnt_filter/ question_answer_err_rate_ori['all_count'],4)
        print("计算无效题目&答案占比: {}({}/{})".format(err_rate_percent,err_cnt_filter, question_answer_err_rate_ori['all_count']))
        r_0="{}({}/{})".format(err_rate_percent,err_cnt_filter, question_answer_err_rate_ori['all_count'])
        # 计算无效题目判断准确率
        if err_cnt_filter==0:
            err_cnt_right_rate=0
        else:
            err_cnt_right_rate=round((err_cnt_filter-question_answer_err_rate_filter['err_count']+( question_answer_err_rate_ori['err_count']-err_cnt_filter))/err_cnt_filter,4)
        up_=err_cnt_filter-question_answer_err_rate_filter['err_count']+(question_answer_err_rate_ori['err_count']-err_cnt_filter)
        down_=err_cnt_filter
        print("无效题目判断准确率: {}({}/{})".format(err_cnt_right_rate,up_,down_))
        r_1="{}({}/{})".format(err_cnt_right_rate,up_,down_)
        # 计算有效题目损失率
        effective_err_rate=round((question_answer_err_rate_filter['err_count']-(question_answer_err_rate_ori['err_count']-err_cnt_filter))/question_answer_err_rate_ori['all_count'],4)
        up_=question_answer_err_rate_filter['err_count']-(question_answer_err_rate_ori['err_count']-err_cnt_filter)
        down_=question_answer_err_rate_ori['all_count']
        print("有效题目损失率: {}({}/{})".format(effective_err_rate,up_,down_))
        r_2="{}({}/{})".format(effective_err_rate,up_,down_)
        print("")
        return [r_0,r_1,r_2]
    

    def filter_by_question_id(self, id, filted_question_idlist):
        new_evalinfo = deepcopy(self._id2evalinfo[id])
        for q_id in filted_question_idlist:
            if q_id in new_evalinfo["ori_data"].keys():
                new_evalinfo["ori_data"].pop(q_id)
            if q_id in new_evalinfo["clean_data"].keys():
                new_evalinfo["clean_data"].pop(q_id)
                new_evalinfo["match_data"].pop(q_id)
                new_evalinfo["match_res"].pop(q_id)
        new_evalinfo["ori_question_idset"] = set(new_evalinfo["ori_data"].keys())
        new_evalinfo["clean_question_idset"] = set(new_evalinfo["clean_data"].keys())
        return new_evalinfo

    def analyze_by_evalinfo(self, eval_info):
        ori_data = eval_info["ori_data"]
        clean_data = eval_info["clean_data"]

        all_count = len(ori_data)
        clean_count = len(clean_data)
        # Show question error rate.
        print ("###题干错误###")
        question_err_rate,_= self.calc_err_rate(all_count-clean_count, all_count)

        # Show answer error rate.
        print ("###答案错误###")
        answer_err_count = 0
        answer_right_count = 0
        for q_id in eval_info["match_res"].keys():
            if eval_info["match_res"][q_id] == "N":
                answer_err_count += 1
            elif eval_info["match_res"][q_id] == "Y":
                answer_right_count += 1
        assert (answer_err_count+answer_right_count) == clean_count
        answer_err_rate,_= self.calc_err_rate(answer_err_count, clean_count)

        # Show answer or question error rate
        print ("###题干或答案错误###")
        question_answer_err_rate,r= self.calc_err_rate(all_count-answer_right_count, all_count)        
        return question_err_rate, answer_err_rate, question_answer_err_rate,r
    


    # Eval the whole dataset
    def eval_dataset(self, filted_question_idlist):
        total_weighted_err_rate = 0
        filted_total_weighted_err_rate = 0
        filted_err_weighted_rate=0
        eval_res={}
        for id in self._id2evalinfo.keys():
            print("id: ",id)
            cur_eval_dict = self._id2evalinfo[id]
            filted_cur_eval_dict = self.filter_by_question_id(id, filted_question_idlist)
            q_err_rate, a_err_rate, q_a_err_rate,o_r= self.analyze_by_evalinfo(cur_eval_dict)
            f_q_err_rate, f_a_err_rate, f_q_a_err_rate,f_r= self.analyze_by_evalinfo(filted_cur_eval_dict)
            total_weighted_err_rate += self._id2info[id]["clean_weight"] * q_a_err_rate["err_rate"]
            filted_total_weighted_err_rate += self._id2info[id]["clean_weight"] * f_q_a_err_rate["err_rate"]
            # single source
            err_rate_percent,err_cnt_right_rate,effective_err_rate=self.calc_ineffective(id,filted_question_idlist)
            filted_err_weighted_rate += self._id2info[id]["clean_weight"] * float(effective_err_rate.split("(")[0])
            eval_res[id]={"ori":o_r,
                           "filter":f_r,
                           "filter_recall":[err_rate_percent,err_cnt_right_rate,effective_err_rate]}
        
        print("题库加权平均错误率为: {}".format(round(total_weighted_err_rate, 4)))
        print("清洗后题库加权平均错误率为: {}".format(round(filted_total_weighted_err_rate, 4)))
        print("清洗后题库加权平均过滤错误率为: {}".format(round(filted_err_weighted_rate,4)))
        eval_res["total"]={"ori":["{}".format(round(total_weighted_err_rate, 4)),"{}".format(round(1-total_weighted_err_rate, 4))],
                           "filter":["{}".format(round(filted_total_weighted_err_rate, 4)),"{}".format(round(1-filted_total_weighted_err_rate, 4))],
                           "filter_recall":["","",filted_err_weighted_rate]
                           }
        return eval_res

    # Eval the whole dataset and save the results to xlsx file
    def eval_dataset_and_save(self,save_path,filted_question_idlist):
        # ｜source_id｜清洗前 Error Rate｜清洗后 Error Rate｜无效题目占比|无效题目判断准确率|有效题目损失率|清洗前 Right Rate｜清洗后 Right Rate｜
        df_save={"source_id":[],"清洗前 Error Rate":[],"清洗后 Error Rate":[],"无效题目占比":[],"无效题目判断准确率":[],"有效题目损失率":[],"清洗前 Right Rate":[],"清洗后 Right Rate":[]}
        eval_res=self.eval_dataset(filted_question_idlist)
        for id_ in eval_res:
            df_save["source_id"].append(id_)
            df_save["清洗前 Error Rate"].append(eval_res[id_]["ori"][0])
            df_save["清洗后 Error Rate"].append(eval_res[id_]["filter"][0])
            df_save["无效题目占比"].append(eval_res[id_]["filter_recall"][0])
            df_save["无效题目判断准确率"].append(eval_res[id_]["filter_recall"][1])
            df_save["有效题目损失率"].append(eval_res[id_]["filter_recall"][2])
            df_save["清洗前 Right Rate"].append(eval_res[id_]["ori"][1])
            df_save["清洗后 Right Rate"].append(eval_res[id_]["filter"][1])
        df_save=pd.DataFrame(df_save)
        df_save.to_excel(save_path,index=False)
        
    
    # Todo: check the bad case for analysis
    def filter_bad_case_for_analysis(self,save_path,filter_qid_list,strategy_labeled_file=None,save_split=False):
        flag=False
        if strategy_labeled_file:
            flag=True
            strategy_labeled_dic={}
            with open(strategy_labeled_file,"r+") as fr:
                strategy_labeled_data=fr.readlines()
            for idx,strategy_labeled_line_data in tqdm(enumerate(strategy_labeled_data),total=len(strategy_labeled_data)):
                strategy_labeled_line_data=json.loads(strategy_labeled_line_data)
                for q_id in strategy_labeled_line_data:
                    q_id_strategy_label=strategy_labeled_line_data[q_id]
                    strategy_labeled_dic[q_id]=q_id_strategy_label
            
        # for every sheet
        # | q_id | prompt | answer_tiku | answer_label | bad_case_type | human_answer_label | type_label | bak_label | human_type_label | human_bak_label | human_match | v4_2_match |
        pd_save_list=[]
        for id_ in self._id2evalinfo:
            pd_save={"q_id":[],"prompt":[],"answer_tiku":[],"answer_label":[],"bad_case_type":[],"human_answer_label":[],"type_label":[],"bak_label":[],"human_type_label":[],"human_match":[],"human_bak_label":[],"v4_2_match":[],"strategy_label":[]}
            ori_data_dic=self._id2evalinfo[id_]["ori_data"]
            clean_data_dic=self._id2evalinfo[id_]["clean_data"]
            match_data_dic=self._id2evalinfo[id_]["match_data"]
            ori_qid_set=self._id2evalinfo[id_]["ori_question_idset"]
            clean_qid_set=self._id2evalinfo[id_]["clean_question_idset"]
            match_qid_set=self._id2evalinfo[id_]["match_question_idset"]
            match_err_qid_set=clean_qid_set-match_qid_set
            # labeled but not filter: 标注了，但是策略没有过滤掉，need: 原始的标注提干有误的q_id_list
            question_err_set=ori_qid_set-clean_qid_set
            filter_qid_set=set(filter_qid_list)
            question_filter_err_set=question_err_set-filter_qid_set
            
            # filter error: 过滤出了原本正确的结果，need: 过滤出来但是human_match为Y的 q_id_list 
            answer_err_set=filter_qid_set-question_err_set-match_err_qid_set
            answer_err_set = answer_err_set.intersection(clean_qid_set)
            
            # save two type of bad case: 保存两种类型的bad case用于分析
            # 标注但是没有过滤
            for question_err_filter_id in question_filter_err_set:
                try:
                    if question_err_filter_id=="":
                        continue
                    pd_save["q_id"].append(question_err_filter_id)
                    pd_save["prompt"].append(ori_data_dic[question_err_filter_id]["prompt"])
                    pd_save["answer_tiku"].append(ori_data_dic[question_err_filter_id]["answer_tiku"])
                    pd_save["answer_label"].append(ori_data_dic[question_err_filter_id]["answer_label"])
                    pd_save["type_label"].append(ori_data_dic[question_err_filter_id]["type_label"])
                    pd_save["bak_label"].append(ori_data_dic[question_err_filter_id]["bak_label"])
                    pd_save["human_type_label"].append("")
                    pd_save["human_answer_label"].append("")
                    pd_save["human_bak_label"].append("")
                    pd_save["human_match"].append("")
                    pd_save["v4_2_match"].append("")
                    pd_save["bad_case_type"].append("标注为无效，但是策略没有过滤")
                    if flag:
                        pd_save["strategy_label"].append(strategy_labeled_dic[question_err_filter_id])
                    else:
                        pd_save["strategy_label"].append("None label!")
                except:
                    print("question_err_filter_id: ",question_err_filter_id)
            
            # 标注有效，但是被过滤
            for answer_err_filter_id in answer_err_set:
                try:
                    if answer_err_filter_id=="":
                        continue
                    pd_save["q_id"].append(answer_err_filter_id)
                    pd_save["prompt"].append(clean_data_dic[answer_err_filter_id]["prompt"])
                    pd_save["answer_tiku"].append(clean_data_dic[answer_err_filter_id]["answer_tiku"])
                    pd_save["answer_label"].append(clean_data_dic[answer_err_filter_id]["answer_label"])
                    pd_save["type_label"].append(clean_data_dic[answer_err_filter_id]["type_label"])
                    pd_save["bak_label"].append(clean_data_dic[answer_err_filter_id]["bak_label"]) 
                    pd_save["human_type_label"].append(match_data_dic[answer_err_filter_id]["type_label"])
                    pd_save["human_answer_label"].append(match_data_dic[answer_err_filter_id]["human_answer_label"])
                    pd_save["human_bak_label"].append(match_data_dic[answer_err_filter_id]["bak_label"])
                    pd_save["human_match"].append(match_data_dic[answer_err_filter_id]["human_match"])
                    pd_save["v4_2_match"].append(match_data_dic[answer_err_filter_id]["v4_2_match"])
                    pd_save["bad_case_type"].append("标注有效，但是被策略过滤") 
                    if flag:
                        pd_save["strategy_label"].append(strategy_labeled_dic[answer_err_filter_id])
                    else:
                        pd_save["strategy_label"].append("None label!")   
                except:
                    print("answer_err_filter_id: ",answer_err_filter_id)
            
            pd_save=pd.DataFrame(pd_save)
            pd_save_list.append([id_,pd_save])
        
        # 保存到一个shell表格里面
        if save_split:
            with pd.ExcelWriter(save_path) as writer:
                for id_,pd_save in pd_save_list:
                    pd_save.to_excel(writer, sheet_name=id_)
        else:
            pd_combined = pd.concat([p[1] for p in pd_save_list],ignore_index=True)
            pd_combined =  pd_combined.to_excel(save_path,index=False)

            

            
if __name__ == "__main__":
    evaluator = QuestionDatasetEvaluator() ###
    evaluator.init() ###
    
    
   

