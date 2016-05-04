package ViewSelection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Stack;


public class Algorithm {
	public static HashSet<FeedInfo> feeds;
    public static HashSet<UserInfo> users;
    
    private static boolean initialized = false;
    
  //For Debug
    public static int debugMatViewCount = 0;
    public static int debugAllViewCount = 0;
    public static int debugAverageFollow = 0;
    public static int debugShareViews = 0;
    
    
    
    public static void init(HashSet<UserInfo> users, HashSet<FeedInfo> feeds){
    	Algorithm.users = users;
    	Algorithm.feeds = feeds;
    	if(users != null && feeds != null)
    		Algorithm.initialized = true;
    }
    
    private static void resetStat(){
    	debugMatViewCount = 0;
    	debugAllViewCount = 0;
    	debugAverageFollow = 0;
    	debugShareViews = 0;
    }
    
    public static HashMap<View,ArrayList<View>> simpleViews(boolean isPush){
    	
    	if(!Algorithm.initialized)
    		return null;
    	resetStat();
    	
        HashMap<View,ArrayList<View>> graph = new HashMap<View,ArrayList<View>>();
        HashMap<Integer,View> feedViews = new HashMap<Integer,View>();
        //ArrayList<HashSet<Integer>> vertex = new ArrayList<HashSet<Integer>>();
        for(FeedInfo fi:feeds){
            HashSet<Integer> fi_set = new HashSet<Integer>();
            fi_set.add(fi.getId());
            View v_fi = new View(fi_set,"F"+String.valueOf(fi.getId()),true,fi.getURL());
            v_fi.updateIntv = fi.getUpdateIntv();
            graph.put(v_fi, null);
            feedViews.put(fi.getId(),v_fi);
        }
        for(UserInfo ui:users){
        	ArrayList<View> follow = new ArrayList<View>();
        	HashSet<String> answer = new HashSet<String>();
            View v_ui = new View(ui.getFollow(),"U"+String.valueOf(ui.getId()),false,ui.getURL());
            for(Integer feed:ui.getFollow()){
            	follow.add(feedViews.get(feed));
            	if(!isPush)
            		answer.add("F"+feed);
            }
            if(isPush){
            	v_ui.isMaterialized = true;
            	answer.add(v_ui.id);
            	ui.setQuery(answer);
            }else{
            	v_ui.isMaterialized = false;
            	v_ui.queryFreq = ui.getRequestIntv();
            	ui.setQuery(answer);
            }
            ui.changePush(isPush);
            graph.put(v_ui, follow);
        }
        
        
        return graph;
    }
    
//    public static HashMap<View,ArrayList<View>> combineViews(){
//    	
//    	if(!Algorithm.initialized)
//    		return null;
//    	resetStat();
//    	
//        //Transitive Reduction of View Graph
//        HashMap<Integer, ArrayList<View>> vertex = new HashMap<Integer, ArrayList<View>>();
//        HashSet<Integer> allFeeds = new HashSet<Integer>();
//        HashMap<View,ArrayList<View>> graph = new HashMap<View,ArrayList<View>>();
//        int max_size = 0;
//        //ArrayList<HashSet<Integer>> vertex = new ArrayList<HashSet<Integer>>();
//        for(FeedInfo fi:feeds){
//            HashSet<Integer> fi_set = new HashSet<Integer>();
//            fi_set.add(fi.getId());
//            if(vertex.containsKey(fi_set.size())) {
//                vertex.get(fi_set.size()).add(new View(fi_set,"F"+String.valueOf(fi.getId()),true,fi.getURL()));
//            }else{
//                ArrayList<View> vertex_group = new ArrayList<View>();
//                vertex_group.add(new View(fi_set,"F"+String.valueOf(fi.getId()),true,fi.getURL()));
//                vertex.put(fi_set.size(),vertex_group);
//                if(fi_set.size()>max_size)
//                    max_size = fi_set.size();
//            }
//        }
//        for(UserInfo ui:users){
//            HashSet<Integer> ui_set = new HashSet<Integer>();
//            ui_set.addAll(ui.getFollow());
//
//            allFeeds.addAll(ui_set);
//            if(vertex.containsKey(ui_set.size()))
//                vertex.get(ui_set.size()).add(new View(ui_set,"U"+String.valueOf(ui.getId()),false,ui.getURL()));
//            else{
//                ArrayList<View> vertex_group = new ArrayList<View>();
//                vertex_group.add(new View(ui_set,"U"+String.valueOf(ui.getId()),false,ui.getURL()));
//                vertex.put(ui_set.size(),vertex_group);
//                if(ui_set.size()>max_size)
//                    max_size = ui_set.size();
//            }
//        }
//        //Calculate the transitive closure
//        for(Map.Entry<Integer,ArrayList<View>> entry:vertex.entrySet()){
//            for(View view:entry.getValue()){
//                if(!graph.containsKey(view)){
//                    ArrayList<View> views = new ArrayList<View>();
//                    graph.put(view,views);
//                }
//                for(int i = entry.getKey();i<=max_size;i++){
//                    if(!vertex.containsKey(i))
//                        continue;
//                    for(View target:vertex.get(i)){
//                        if(target.feeds.containsAll(view.feeds) && target != view && !target.isFeed)
//                            graph.get(view).add(target);
//                    }
//                }
//            }
//        }
//
//        //Calculate the transitive reduction
//        for(View fromV:graph.keySet()) {
//            for (View midV : graph.keySet()) {
//                if(midV == fromV || midV.feeds.size() <= fromV.feeds.size()||!graph.get(fromV).contains(midV))
//                    continue;
//                for (View toV : graph.keySet()) {
//                    if (toV == midV || toV == fromV)
//                        continue;
//                    else if(!inPath(midV,toV,graph))
//                        continue;
//                    else{
//                        if(graph.get(fromV).contains(toV))
//                            graph.get(fromV).remove(toV);
//                    }
//                }
//            }
//        }
//        
//        HashMap<View,ArrayList<View>> fromgraph = new HashMap<View,ArrayList<View>>();
//        for(Map.Entry<View,ArrayList<View>> e:graph.entrySet()){
//        	for(View to:e.getValue()){
//        		if(fromgraph.containsKey(to)){
//        			fromgraph.get(to).add(e.getKey());
//        		}else{
//        			ArrayList<View> list = new ArrayList<View>();
//        			list.add(e.getKey());
//        			fromgraph.put(to, list);
//        		}
//        	}
//        }
//        return fromgraph;
//    }

//    private static boolean inPath(View midV, View toV,
//			HashMap<View, ArrayList<View>> graph) {
//		if(graph.get(midV).contains(toV))
//			return true;
//		else{
//			if(graph.get(midV).isEmpty())
//				return false;
//			else{
//				for(View v:graph.get(midV))
//					return inPath(v,toV,graph);
//				return false;
//			}
//		}
//		
//	}
    
    public static HashMap<View,ArrayList<View>> hierachyGreedyViews(){
    	if(!Algorithm.initialized)
    		return null;
    	resetStat();
    	
		HashMap<Integer, HashSet<View>> vertex = new HashMap<Integer, HashSet<View>>();
        HashMap<String, View> viewmap = new HashMap<String,View>();
        HashMap<View,ArrayList<View>> graph = new HashMap<View,ArrayList<View>>();
        HashMap<Integer,Double> feedFreq = new HashMap<Integer,Double>();
        int max_size = 0;
        //int totalview = feeds.size();
        //ArrayList<HashSet<Integer>> vertex = new ArrayList<HashSet<Integer>>();
        for(FeedInfo fi:feeds){
            HashSet<Integer> fi_set = new HashSet<Integer>();
            fi_set.add(fi.getId());
            feedFreq.put(fi.getId(), 1 / Double.valueOf(fi.getUpdateIntv()));
            View nv = new View(fi_set,"F"+String.valueOf(fi.getId()),true,fi.getURL());
            nv.isMaterialized = true;
            if(vertex.containsKey(fi_set.size())) {     	
            	nv.updateFreq = 1 / Double.valueOf(fi.getUpdateIntv());
            	nv.updateIntv = fi.getUpdateIntv();
            	nv.queryFreq = 0;
                vertex.get(fi_set.size()).add(nv);
            }else{
                HashSet<View> vertex_group = new HashSet<View>();
            	nv.updateFreq = 1 / Double.valueOf(fi.getUpdateIntv());
            	nv.updateIntv = fi.getUpdateIntv();
            	nv.queryFreq = 0;
                vertex_group.add(nv);
                vertex.put(fi_set.size(),vertex_group);
                if(fi_set.size()>max_size)
                    max_size = fi_set.size();
            }
            viewmap.put(nv.id, nv);
            debugMatViewCount++;
        }
        
        for(UserInfo ui:users){
            HashSet<Integer> ui_set = new HashSet<Integer>();
            ui_set.addAll(ui.getFollow());
            debugAverageFollow += ui.getFollow().size();
        	View nv = new View(ui_set,"U"+String.valueOf(ui.getId()),false,ui.getURL());
            if(vertex.containsKey(ui_set.size())){
            	//Combine duplicate views
            	if(vertex.get(ui_set.size()).contains(nv)){
            		for(View n:vertex.get(ui_set.size())){
            			if(n.equals(nv)){
            				n.queryFreq += 1 / Double.valueOf(ui.getRequestIntv());
            				n.setAlias(ui.getId());
            				ui.setAlias(Integer.valueOf(n.id.substring(1)));
            				debugShareViews++;
            				break;
            			}
            		}
            	}else{
            		nv.updateFreq = 0;
            		for(Integer f:ui.getFollow())
            			nv.updateFreq += feedFreq.get(f);
            		nv.queryFreq = 1 / Double.valueOf(ui.getRequestIntv());
            		vertex.get(ui_set.size()).add(nv);
            		viewmap.put(nv.id, nv);
            	}
            }else{
                HashSet<View> vertex_group = new HashSet<View>();
            	nv.updateFreq = 0;
        		for(Integer f:ui.getFollow())
        			nv.updateFreq += feedFreq.get(f);
        		nv.queryFreq = 1 / Double.valueOf(ui.getRequestIntv());
                vertex_group.add(nv);
                vertex.put(ui_set.size(),vertex_group);
                if(ui_set.size()>max_size)
                    max_size = ui_set.size();
                viewmap.put(nv.id, nv);
            }
        }
        
        if(vertex.keySet().contains(null)){
        	System.out.println("null vertex");
        }
        
        if(viewmap.containsKey(null)){
        	System.out.println("null view");
        }
        
        debugAllViewCount = viewmap.size();
        //Calculate the transitive closure
        for(Map.Entry<Integer,HashSet<View>> entry:vertex.entrySet()){
            for(View view:entry.getValue()){
                if(!graph.containsKey(view)){
                    ArrayList<View> views = new ArrayList<View>();
                    graph.put(view,views);
                }
                for(int i = entry.getKey();i<=max_size;i++){
                    if(!vertex.containsKey(i))
                        continue;
                    for(View target:vertex.get(i)){
                        if(target.feeds.containsAll(view.feeds) && target != view && !target.isFeed)
                            graph.get(view).add(target);
                    }
                }
            }
        }
        
        //Display the generated graph
//      for(View from:graph.keySet()) {
//	      System.out.print(from.id + ":\t");
//	      for (View to : graph.get(from)) {
//	          System.out.print(to.id + "\t");
//	      }
//	      System.out.println();
//      }
//      System.out.println();
//      System.out.println();
        
        //Candidate view generation finished
        
        if(graph.containsKey(null))
        	System.out.println("null key");
        
        //Reverse the adjacency matrix
        HashMap<View,ArrayList<View>> fromgraph = new HashMap<View,ArrayList<View>>();
        for(Map.Entry<View,ArrayList<View>> e:graph.entrySet()){
        	if(!fromgraph.containsKey(e.getKey())){
	        	ArrayList<View> elist = new ArrayList<View>();
	        	fromgraph.put(e.getKey(), elist);
        	}
        	for(View to:e.getValue()){
        		if(to == null)
        			continue;
        		if(fromgraph.containsKey(to)){
        			fromgraph.get(to).add(e.getKey());
        		}else{
        			ArrayList<View> list = new ArrayList<View>();
        			list.add(e.getKey());
        			fromgraph.put(to, list);
        		}
        	}
        }
        
      //DEBUG
//        for(View from:fromgraph.keySet()) {
//      	  if(from.isMaterialized)
//      		  System.out.print("(M)");
//  	      System.out.print(from.id + ":\t");
//  	      if(fromgraph.get(from) == null){
//  	    	  System.out.println();
//  	    	  continue;
//  	      }
//  	      for (View to : fromgraph.get(from)) {
//  	          System.out.print(to.id + "\t");
//  	      }
//  	      System.out.println();
//        }
//        System.out.println();
        
        //Greedy Materialization Algorithm
        //1. Calculate the materialization order using DFS on fromgraph
        ArrayList<View> candidateViews = new ArrayList<View>();
        HashSet<View> visitedView = new HashSet<View>();
        Stack<View> dfs = new Stack<View>();
        for(View v:vertex.get(max_size)){
        	dfs.push(v);
        	while(!dfs.isEmpty()){
        		View t = dfs.peek();
        		if(visitedView.contains(t)){
        			dfs.pop();
        			candidateViews.add(t);
        		}else{
        			visitedView.add(t);
        			if(!fromgraph.get(t).isEmpty())
	        			for(View from:fromgraph.get(t))
	        				dfs.push(from);
        		}
        	}
        }
        int submax = max_size - 1;
        while(visitedView.size() != viewmap.size()){
        	if(!vertex.containsKey(submax)){
        		submax--;
        		continue;
        	}
        		
        	for(View v:vertex.get(submax)){
            	dfs.push(v);
            	while(!dfs.isEmpty()){
            		View t = dfs.peek();
            		if(visitedView.contains(t)){
            			dfs.pop();
            			if(!candidateViews.contains(t))
            				candidateViews.add(t);
            		}else{
            			visitedView.add(t);
            			for(View from:fromgraph.get(t))
            				dfs.push(from);
            		}
            	}
            }
        	submax--;
        }
        //2. Initialize the view selection plan as all pull from feeds
        HashMap<View,ArrayList<View>> returngraph = new HashMap<View,ArrayList<View>>();
        for(UserInfo ui:users){
        	String id = String.valueOf(ui.getId());
        	if(!viewmap.containsKey("U"+String.valueOf(ui.getId()))){
        		id = String.valueOf(ui.getAlias());
        	}
        	ArrayList<View> follows = new ArrayList<View>();
        	for(int fi:ui.getFollow())
        		follows.add(viewmap.get("F"+fi));
        	returngraph.put(viewmap.get("U"+id), follows);
        }
        
        //3. Calculate whether an view should be materialized in bottom-up order
        HashSet<String> materialized = new HashSet<String>();
        for(FeedInfo fi:feeds){
        	materialized.add("F"+fi.getId());
        	returngraph.put(viewmap.get("F"+fi.getId()), new ArrayList<View>());
        }
        for(View tomat:candidateViews){
        	if(tomat.isFeed)
        		continue;
        	if(!viewmap.containsKey(tomat.id))
        		System.out.println("Error, unknown view: " + tomat.id);
        	if(viewmap.get(tomat.id).isProcessed)
        		continue;
        	else
        		viewmap.get(tomat.id).isProcessed = true;
        	if(decision(returngraph,viewmap.get(tomat.id),materialized,fromgraph,graph)){
        		viewmap.get(tomat.id).isMaterialized = true;
        		materialized.add(tomat.id);
        		newMaterialized(returngraph,viewmap.get(tomat.id),materialized,fromgraph,graph,viewmap);//Add tomat to returngraph, edit all the views that could benefit from tomat
        	}else{
        		tomat.isMaterialized = false;//Add tomat to returngraph, calculate the minimum set cover for tomat.feeds()
        		newVirtual(returngraph,viewmap.get(tomat.id),materialized,fromgraph,true);
        	}
        }
        debugMatViewCount = materialized.size();
        
        //4. Assign generated view selection plan to each user's query
        for(UserInfo ui:users){
        	int id = ui.getId();
        	if(!viewmap.containsKey("U"+String.valueOf(ui.getId()))){
        		id = ui.getAlias();
        	}
        	
        	if(viewmap.get("U"+String.valueOf(id)).isMaterialized)
        		ui.setPush(true);
        	else{
        		HashMap<String,String> pullViews = new HashMap<String,String>();
        		if(!returngraph.containsKey(viewmap.get("U"+String.valueOf(id))) || returngraph.get(viewmap.get("U"+String.valueOf(id))) == null){
        			for(Integer f:ui.getFollow())
        				pullViews.put("F"+f, ui.getURL());
        		}else{
	        		for(View v:returngraph.get(viewmap.get("U"+String.valueOf(id)))){
	        			if(v == null)
	        				continue;
	            		pullViews.put(v.id, v.URL);
	            	}
        		}
        		ui.setPull(pullViews);
        	}
        	HashSet<String> sources = new HashSet<String>();
        	for(View v:returngraph.get(viewmap.get("U"+String.valueOf(id)))){
        		if(v == null)
        			continue;
        		sources.add(v.id);
        	}
        	ui.setQuery(sources);
        }
        
        //DEBUG
//        for(View from:returngraph.keySet()) {
//      	  if(from.isMaterialized)
//      		  System.out.print("(M)");
//  	      System.out.print(from.id + ":\t");
//  	      if(returngraph.get(from) == null){
//  	    	  System.out.println();
//  	    	  continue;
//  	      }
//  	      for (View to : returngraph.get(from)) {
//  	          System.out.print(to.id + "\t");
//  	      }
//  	      System.out.println();
//        }
//        System.out.println();
        
		return returngraph;
	}
	private static int newVirtual(HashMap<View, ArrayList<View>> returngraph,
			View v, HashSet<String> materialized, HashMap<View, ArrayList<View>> fromgraph, boolean calc) {
		
		HashSet<Integer> allfollow = new HashSet<Integer>();
		ArrayList<View> candidateView = new ArrayList<View>();
		ArrayList<View> selectView = new ArrayList<View>();
		//List all materialized view that could answer part of v's query
		int size = fromgraph.get(v).size();
		for(View sourcev:fromgraph.get(v)){
			if(!materialized.contains(sourcev.id))
				continue;
			candidateView.add(sourcev);
		}
		//Remove all the materialized views which is wholly contained by other one.
		ArrayList<View> removeView = new ArrayList<View>();
		for(View matv:candidateView){
			for(View matv2:candidateView){
				if(matv2.id == matv.id)
					continue;
				else if(matv.feeds.containsAll(matv2.feeds)){
					removeView.add(matv2);
				}
			}
		}
		candidateView.removeAll(removeView);
		
		while(!allfollow.containsAll(v.feeds)){
			Collections.sort(candidateView, new viewSizeComparator());
			if(!allfollow.containsAll(candidateView.get(0).feeds)){
					allfollow.addAll(candidateView.get(0).feeds);
					selectView.add(candidateView.get(0));
			}
			candidateView.remove(0);
		}
		if(returngraph.get(v).size() >= selectView.size()){
			if(calc){
				returngraph.put(v, selectView);
				v.links.clear();
				v.links.addAll(selectView);
			}
			return returngraph.get(v).size() - selectView.size();
		}else
			return 0;
		
	}

	private static void newMaterialized(HashMap<View, ArrayList<View>> returngraph,
			View view, HashSet<String> materialized, HashMap<View, ArrayList<View>> fromgraph, HashMap<View, ArrayList<View>> graph, HashMap<String, View> viewmap) {
		returngraph.put(view,returngraph.get(view)); //Update isMaterialized flag for view in returngraph
		for(View to:graph.get(view)){
			if(!viewmap.get(to.id).isMaterialized)
				newVirtual(returngraph,to,materialized,fromgraph,true);
		}
		
	}

	private static boolean decision(HashMap<View, ArrayList<View>> returngraph,
			View view, HashSet<String> materialized, HashMap<View, ArrayList<View>> fromgraph, HashMap<View, ArrayList<View>> graph) {
		double uc_inc = view.updateFreq * Simulator.costratio;
		double ev_dec = 0;
		materialized.add(view.id);
		for(View to:graph.get(view)){
			if(!materialized.contains(to.id))
				ev_dec += to.queryFreq * newVirtual(returngraph,to,materialized,fromgraph,false);
		}
		materialized.remove(view.id);
		return ev_dec > uc_inc;
	}
	
	public static HashMap<View, ArrayList<View>> greedyViews() {
		if(!Algorithm.initialized)
			return null;
		resetStat();
		
		HashMap<Integer, HashSet<View>> vertex = new HashMap<Integer, HashSet<View>>();
        HashMap<String, View> viewmap = new HashMap<String,View>();
        HashMap<View,ArrayList<View>> graph = new HashMap<View,ArrayList<View>>();
        HashMap<Integer,Double> feedFreq = new HashMap<Integer,Double>();
        int max_size = 0;
        //ArrayList<HashSet<Integer>> vertex = new ArrayList<HashSet<Integer>>();
        for(FeedInfo fi:feeds){
            HashSet<Integer> fi_set = new HashSet<Integer>();
            fi_set.add(fi.getId());
            feedFreq.put(fi.getId(), 1 / Double.valueOf(fi.getUpdateIntv()));
            View nv = new View(fi_set,"F"+String.valueOf(fi.getId()),true,fi.getURL());
            nv.isMaterialized = true;
            if(vertex.containsKey(fi_set.size())) {     	
            	nv.updateFreq = 1 / Double.valueOf(fi.getUpdateIntv());
            	nv.updateIntv = fi.getUpdateIntv();
            	nv.queryFreq = 0;
                vertex.get(fi_set.size()).add(nv);
            }else{
                HashSet<View> vertex_group = new HashSet<View>();
            	nv.updateFreq = 1 / Double.valueOf(fi.getUpdateIntv());
            	nv.updateIntv = fi.getUpdateIntv();
            	nv.queryFreq = 0;
                vertex_group.add(nv);
                vertex.put(fi_set.size(),vertex_group);
                if(fi_set.size()>max_size)
                    max_size = fi_set.size();
            }
            viewmap.put(nv.id, nv);
            debugMatViewCount++;
        }
        for(UserInfo ui:users){
            HashSet<Integer> ui_set = new HashSet<Integer>();
            ui_set.addAll(ui.getFollow());
            debugAverageFollow += ui.getFollow().size();
        	View nv = new View(ui_set,"U"+String.valueOf(ui.getId()),false,ui.getURL());
            if(vertex.containsKey(ui_set.size())){
            	//Combine duplicate views
            	if(vertex.get(ui_set.size()).contains(nv)){
            		for(View n:vertex.get(ui_set.size())){
            			if(n.equals(nv)){
            				n.queryFreq += 1 / Double.valueOf(ui.getRequestIntv());
            				n.setAlias(ui.getId());
            				ui.setAlias(Integer.valueOf(n.id.substring(1)));
            				debugShareViews++;
            				break;
            			}
            		}
            	}else{
            		nv.updateFreq = 0;
            		for(Integer f:ui.getFollow())
            			nv.updateFreq += feedFreq.get(f);
            		nv.queryFreq = 1 / Double.valueOf(ui.getRequestIntv());
            		vertex.get(ui_set.size()).add(nv);
            		viewmap.put(nv.id, nv);
            	}
            }else{
                HashSet<View> vertex_group = new HashSet<View>();
            	nv.updateFreq = 0;
        		for(Integer f:ui.getFollow())
        			nv.updateFreq += feedFreq.get(f);
        		nv.queryFreq = 1 / Double.valueOf(ui.getRequestIntv());
                vertex_group.add(nv);
                vertex.put(ui_set.size(),vertex_group);
                if(ui_set.size()>max_size)
                    max_size = ui_set.size();
                viewmap.put(nv.id, nv);
            }
        }
        
        if(vertex.keySet().contains(null)){
        	System.out.println("null vertex");
        }
        
        if(viewmap.containsKey(null)){
        	System.out.println("null view");
        }
        
        debugAllViewCount = viewmap.size();
        //Calculate the transitive closure
        for(Map.Entry<Integer,HashSet<View>> entry:vertex.entrySet()){
            for(View view:entry.getValue()){
                if(!graph.containsKey(view)){
                    ArrayList<View> views = new ArrayList<View>();
                    graph.put(view,views);
                }
                for(int i = entry.getKey();i<=max_size;i++){
                    if(!vertex.containsKey(i))
                        continue;
                    for(View target:vertex.get(i)){
                        if(target.feeds.containsAll(view.feeds) && target != view && !target.isFeed)
                            graph.get(view).add(target);
                    }
                }
            }
        }
        //Candidate view generation finished
        
        //DEBUG
        //Display the generated graph
//      for(View from:graph.keySet()) {
//	      System.out.print(from.id + ":\t");
//	      for (View to : graph.get(from)) {
//	          System.out.print(to.id + "\t");
//	      }
//	      System.out.println();
//      }
//      System.out.println();
        if(graph.containsKey(null))
        	System.out.println("null key");
        
        //Reverse the adjacency matrix
        HashMap<View,ArrayList<View>> fromgraph = new HashMap<View,ArrayList<View>>();
        for(Map.Entry<View,ArrayList<View>> e:graph.entrySet()){
        	for(View to:e.getValue()){
        		if(fromgraph.containsKey(to)){
        			fromgraph.get(to).add(e.getKey());
        		}else{
        			ArrayList<View> list = new ArrayList<View>();
        			list.add(e.getKey());
        			fromgraph.put(to, list);
        		}
        	}
        }
        
        //Greedy Materialization Algorithm
        //1. Calculate the benefit cost ratio in descending order
        ArrayList<View> bpc = new ArrayList<View>();
        bpc.addAll(fromgraph.keySet());
        HashSet<String> materialized = new HashSet<String>();
        for(View v:bpc){
        	v.bpc =  v.queryFreq / (v.updateFreq*Simulator.costratio);
        	int linkcount;
        	if(v.links.isEmpty())
        		linkcount = v.feeds.size();
        	else
        		linkcount = v.links.size();
        	v.bpc *= linkcount;
        	
        	
        }
        while(true && !bpc.isEmpty()){
	        Collections.sort(bpc);
	        View tomat = bpc.get(0);
	        if(tomat == null)
	        	System.out.println("null mat");
	        if(viewmap.get(tomat.id).bpc <= 1)
	        	break;
	        viewmap.get(tomat.id).isMaterialized = true;
	        materialized.add(tomat.id);
	        if(tomat.isFeed)
	        	break;
	        for(View v:graph.get(tomat)){
	        	if(viewmap.get(v.id).links.isEmpty()){
	        		viewmap.get(v.id).links.add(tomat);
	        		HashSet<Integer> allfollow = new HashSet<Integer>();
	        		allfollow.addAll(v.feeds);
	        		allfollow.removeAll(tomat.feeds);
	        		for(Integer f:allfollow){
	        			View tv = viewmap.get("F"+String.valueOf(f));
	        			viewmap.get(v.id).links.add(tv);
	        			if(tv == null)
	        				System.out.println("null feed1");
	        		}
	        	}else{
	        		HashSet<Integer> allfollow = new HashSet<Integer>();
	        		ArrayList<View> candidateView = new ArrayList<View>();
	        		//List all materialized view that could answer part of v's query
	        		for(View sourcev:fromgraph.get(v)){
	        			if(!viewmap.get(sourcev.id).isMaterialized)
	        				continue;
	        			candidateView.add(sourcev);
	        		}
	        		//Remove all the materialized views which is wholly contained by other one.
	        		ArrayList<View> removeView = new ArrayList<View>();
	        		for(View matv:candidateView){
	        			for(View matv2:candidateView){
	        				if(matv2.id == matv.id)
	        					continue;
	        				else if(matv.feeds.containsAll(matv2.feeds)){
	        					removeView.add(matv2);
	        				}
	        			}
	        		}
	        		candidateView.removeAll(removeView);
	        		
	        		//Greedy set cover, choosing biggest set first
	        		while(!allfollow.containsAll(v.feeds)){
	        			Collections.sort(candidateView, new viewSizeComparator());
	        			if(!allfollow.containsAll(candidateView.get(0).feeds)){
	        					allfollow.addAll(candidateView.get(0).feeds);
	        					View tv = viewmap.get(candidateView.get(0).id);
	        					viewmap.get(v.id).links.add(tv);
	        					if(tv == null)
	    	        				System.out.println("null feed2");
	        			}
	        			candidateView.remove(0);
	        		}
	        	}
	        	double tbpc = viewmap.get(v.id).queryFreq / (viewmap.get(v.id).updateFreq*Simulator.costratio);
	        	int linkcount;
	        	if(viewmap.get(v.id).links.isEmpty())
	        		linkcount = viewmap.get(v.id).feeds.size();
	        	else
	        		linkcount = viewmap.get(v.id).links.size();
	        	viewmap.get(v.id).bpc = tbpc * linkcount;
	        }
	        	
	        bpc.remove(0);
        }
        debugMatViewCount = materialized.size();
        
        HashMap<View,ArrayList<View>> returngraph = new HashMap<View,ArrayList<View>>();
        
        for(Map.Entry<String, View> e:viewmap.entrySet()){
        	if(e.getValue() == null)
        		System.out.println("Null View: " + e.getKey());
        	if(e.getValue().isFeed)
        		returngraph.put(e.getValue(), null);
        	else if(e.getValue().links.isEmpty()){
        		ArrayList<View> src = new ArrayList<View>();
        		for(Integer f:e.getValue().feeds){
        			src.add(viewmap.get("F"+f));
        		}
        		if(src.contains(null))
        			System.out.println("null source1");
        		returngraph.put(e.getValue(), src);
        	}else{
        		ArrayList<View> src = new ArrayList<View>();
        		src.addAll(e.getValue().links);
        		if(src.contains(null))
        			System.out.println("null source2");
        		returngraph.put(e.getValue(), src);
        	}
        }
        
        
        for(UserInfo ui:users){
        	int id = ui.getId();
        	if(!viewmap.containsKey("U"+String.valueOf(ui.getId()))){
        		id = ui.getAlias();
        	}
        	
        	if(viewmap.get("U"+String.valueOf(id)).isMaterialized)
        		ui.setPush(true);
        	else{
        		HashMap<String,String> pullViews = new HashMap<String,String>();
        		if(!returngraph.containsKey(viewmap.get("U"+String.valueOf(id))) || returngraph.get(viewmap.get("U"+String.valueOf(id))) == null){
        			for(Integer f:ui.getFollow())
        				pullViews.put("F"+f, ui.getURL());
        		}else{
	        		for(View v:returngraph.get(viewmap.get("U"+String.valueOf(id)))){
	        			if(v == null)
	        				continue;
	            		pullViews.put(v.id, v.URL);
	            	}
        		}
        		ui.setPull(pullViews);
        	}
        	HashSet<String> sources = new HashSet<String>();
        	for(View v:returngraph.get(viewmap.get("U"+String.valueOf(id)))){
        		if(v == null)
        			continue;
        		sources.add(v.id);
        	}
        	ui.setQuery(sources);
        }
        
        //DEBUG
        //Display the generated graph
//      for(View from:returngraph.keySet()) {
//    	  if(from.isMaterialized)
//    		  System.out.print("(M)");
//	      System.out.print(from.id + ":\t");
//	      if(returngraph.get(from) == null){
//	    	  System.out.println();
//	    	  continue;
//	      }
//	      for (View to : returngraph.get(from)) {
//	          System.out.print(to.id + "\t");
//	      }
//	      System.out.println();
//      }
//      System.out.println();
        
        
		return returngraph;
	}
}

class viewSizeComparator implements Comparator<View>{

	@Override
	public int compare(View v1, View v2) {
		return v2.feeds.size() - v1.feeds.size();
	}
	
}